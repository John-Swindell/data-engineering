import unittest
from unittest.mock import MagicMock
import pandas as pd
import os

# Make sure this import points to your main script file
from main_pipeline import GCSCachingManager 

class TestGCSCachingManager(unittest.TestCase):

    def setUp(self):
        """This method runs before each test, setting up a clean environment."""
        self.project_id = 'test-project'
        self.bucket_name = 'test-bucket'
        self.local_cache_dir = 'test_cache'
        
        # Mock the GCS client and its methods
        self.mock_storage_client = MagicMock()
        self.mock_bucket = MagicMock()
        self.mock_blob = MagicMock()

        self.mock_storage_client.bucket.return_value = self.mock_bucket
        self.mock_bucket.blob.return_value = self.mock_blob
        
        # Initialize class with the MOCK client
        self.cacher = GCSCachingManager(
            project_id=self.project_id,
            bucket_name=self.bucket_name,
            local_cache_dir=self.local_cache_dir,
            gcs_client=self.mock_storage_client
        )
        
        # Ensure the local test cache directory is clean before each test
        if os.path.exists(self.local_cache_dir):
            for f in os.listdir(self.local_cache_dir):
                os.remove(os.path.join(self.local_cache_dir, f))
            os.rmdir(self.local_cache_dir)
        os.makedirs(self.local_cache_dir, exist_ok=True)

    def tearDown(self):
        """This method runs after each test to clean up."""
        if os.path.exists(self.local_cache_dir):
            for f in os.listdir(self.local_cache_dir):
                os.remove(os.path.join(self.local_cache_dir, f))
            os.rmdir(self.local_cache_dir)

    def test_cache_miss_scenario(self):
        """Test Case 1: Verify behavior when a file is NOT in the cache."""
        print("\nTesting Cache MISS...")
        self.mock_blob.exists.return_value = False
        
        result = self.cacher.get('non_existent_file.parquet')
        
        self.assertIsNone(result)
        self.mock_blob.download_to_filename.assert_not_called()

    def test_cache_hit_from_gcs_scenario(self):
        """Test Case 2: Verify behavior when a file IS in the GCS cache."""
        print("\nTesting Cache HIT from GCS...")
        
        # Arrange: Configure the mock blob to "exist" in GCS
        self.mock_blob.exists.return_value = True

        # Configures the mocked download function to "create" the file when called.
        def simulate_download(local_path):
            dummy_df = pd.DataFrame({'a': [1, 2]})
            dummy_df.to_parquet(local_path)
            
        self.mock_blob.download_to_filename.side_effect = simulate_download
        local_path = os.path.join(self.local_cache_dir, 'test_file.parquet')

        # Act: Get the file
        result = self.cacher.get('test_file.parquet')
        
        # Assert: Check that the download was attempted and the data is correct
        self.mock_blob.download_to_filename.assert_called_once_with(local_path)
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertEqual(result.shape, (2, 1))

    def test_cache_set_scenario(self):
        """Test Case 3: Verify that setting a cache item uploads it to GCS."""
        print("\nTesting Cache SET...")
        
        dummy_df_to_set = pd.DataFrame({'b': [3, 4]})
        file_name = 'new_file.parquet'
        local_path = os.path.join(self.local_cache_dir, file_name)

        self.cacher.set(file_name, dummy_df_to_set)
        
        self.assertTrue(os.path.exists(local_path))
        self.mock_bucket.blob.assert_called_once_with(file_name)
        self.mock_blob.upload_from_filename.assert_called_once_with(local_path)

if __name__ == '__main__':
    unittest.main()