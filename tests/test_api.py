import unittest

from batch import api


class ApiTests(unittest.TestCase):
    def setUp(self):
        self.models = api.MODELS.copy()
        api.MODELS.clear()

    def tearDown(self):
        api.MODELS.clear()
        api.MODELS.update(self.models)

    def test_get_model_returns_model(self):
        api.MODELS["abcd1234"] = {
            "id": "abcd1234",
            "name": "purchases",
            "schema": "{}",
        }

        model = api.get_model("abcd1234")

        self.assertEqual(model["name"], "purchases")

    def test_delete_model_returns_no_content(self):
        api.MODELS["abcd1234"] = {
            "id": "abcd1234",
            "name": "purchases",
            "schema": "{}",
        }

        response = api.delete_model("abcd1234")

        self.assertEqual(response.status_code, 204)
        self.assertNotIn("abcd1234", api.MODELS)
