from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch
from lib.public_dataset import PREFIX, download_files, list_dataset_files
import importlib.util

spec = importlib.util.spec_from_file_location('public_download', Path(__file__).resolve().parents[1]/'download_public-dataset.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def client_for(*names):
    client = Mock(spec=['get_paginator', 'download_file', 'close'])
    client.get_paginator.return_value.paginate.return_value = [
        {'Contents': [{'Key': PREFIX+name} for name in names]}]
    def transfer(bucket, key, target):
        Path(target).write_text('remote '+key.rsplit('/',1)[-1])
    client.download_file.side_effect = transfer
    return client


class PublicDownloadTests(unittest.TestCase):
    def test_pagination_and_filtering(self):
        client=client_for()
        client.get_paginator.return_value.paginate.return_value=[{}, {'Contents':[{'Key':PREFIX+n} for n in ['A.csv','B.log','ignore.txt','nested/C.csv','../bad.csv','.hidden.csv']]}, {'Contents':[{'Key':PREFIX+'D.csv'}]}]
        self.assertEqual([n for _,_,n in list_dataset_files(client)],['A.csv','B.log','D.csv'])

    def test_fresh_setup_routes_files_without_prompt(self):
        with tempfile.TemporaryDirectory() as folder, patch('builtins.input') as ask, patch('builtins.print'):
            root=Path(folder)
            self.assertEqual(download_files(client_for('A.csv','B.log'),root),0)
            self.assertEqual((root/'data/A.csv').read_text(),'remote A.csv')
            self.assertEqual((root/'log/B.log').read_text(),'remote B.log')
            ask.assert_not_called()

    def test_each_existing_file_gets_exact_prompt(self):
        with tempfile.TemporaryDirectory() as folder, patch('builtins.print'):
            root=Path(folder); (root/'data').mkdir(); (root/'log').mkdir()
            (root/'data/A.csv').write_text('keep'); (root/'log/B.log').write_text('replace')
            client=client_for('A.csv','B.log')
            with patch('builtins.input',side_effect=['n','y']) as ask:
                self.assertEqual(download_files(client,root),0)
            self.assertEqual([c.args[0] for c in ask.call_args_list],['overwrite A.csv (y/n)? ','overwrite B.log (y/n)? '])
            self.assertEqual((root/'data/A.csv').read_text(),'keep')
            self.assertEqual((root/'log/B.log').read_text(),'remote B.log')
            client.download_file.assert_called_once()

    def test_bad_answer_reprompts_and_eof_skips(self):
        with tempfile.TemporaryDirectory() as folder, patch('builtins.print'):
            root=Path(folder); (root/'data').mkdir(); (root/'data/A.csv').write_text('keep')
            for responses in [['wrong','n'],[EOFError()]]:
                client=client_for('A.csv')
                with patch('builtins.input',side_effect=responses):
                    self.assertEqual(download_files(client,root),0)
                client.download_file.assert_not_called()

    def test_partial_failure_preserves_file_and_continues(self):
        with tempfile.TemporaryDirectory() as folder, patch('builtins.input',return_value='y'), patch('builtins.print'):
            root=Path(folder); (root/'data').mkdir(); (root/'data/A.csv').write_text('original')
            client=client_for('A.csv','B.csv')
            def transfer(bucket,key,target):
                Path(target).write_text('new')
                if key.endswith('A.csv'): raise OSError('network failure')
            client.download_file.side_effect=transfer
            self.assertEqual(download_files(client,root),1)
            self.assertEqual((root/'data/A.csv').read_text(),'original')
            self.assertEqual((root/'data/B.csv').read_text(),'new')
            self.assertFalse(list((root/'data').glob('*.tmp')))

    def test_dry_run_no_writes_or_prompt(self):
        with tempfile.TemporaryDirectory() as folder, patch('builtins.input') as ask, patch('builtins.print'):
            client=client_for('A.csv')
            self.assertEqual(download_files(client,Path(folder),dry_run=True),0)
            self.assertEqual(list(Path(folder).iterdir()),[])
            ask.assert_not_called();client.download_file.assert_not_called()

    def test_interruption_cleans_temp_and_closes_client(self):
        with tempfile.TemporaryDirectory() as folder, patch('builtins.print'):
            root=Path(folder); config=root/'private.ini'
            config.write_text(f'[DataPort]\napi_key = test\nsecret = test\n[Paths]\nroot = {root}\n')
            client=client_for('A.csv');client.download_file.side_effect=KeyboardInterrupt()
            with patch('boto3.client',return_value=client):
                self.assertEqual(runner.main(['--config',str(config)]),130)
            client.close.assert_called_once()
            self.assertEqual(list((root/'data').iterdir()),[])

    def test_file_appearing_during_transfer_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as folder, patch('builtins.input',return_value='n') as ask, patch('builtins.print'):
            root=Path(folder);client=client_for('A.csv')
            def transfer(bucket,key,target):
                Path(target).write_text('remote');(root/'data/A.csv').write_text('local')
            client.download_file.side_effect=transfer
            self.assertEqual(download_files(client,root),0)
            ask.assert_called_once_with('overwrite A.csv (y/n)? ')
            self.assertEqual((root/'data/A.csv').read_text(),'local')

    def test_empty_listing_is_error(self):
        with self.assertRaises(ValueError): download_files(client_for(),Path('unused'))

if __name__=='__main__': unittest.main()
