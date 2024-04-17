import unittest
import os
from ingestion import prepare_prices_stock, prepare_products

# using unittest intead of pytest simply not to install new libs
class TestIngestionFuncs(unittest.TestCase):
    def test_prepare_proces_csv(self):
        df = prepare_prices_stock("mock_data/MOCK-PRICES_STOCK.csv")
        self.assertEqual(len(df), 6)

    def test_prepare_products(self):
        df = prepare_products("mock_data/MOCK-TEST-PRODUCTS.csv")
        self.assertEqual(len(df), 40)
        self.assertEqual(len(df.columns), 10)


unittest.main()
