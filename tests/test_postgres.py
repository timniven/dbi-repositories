import unittest

from tests.implementations import TweetRepository


class TestPostgresRepository(unittest.TestCase):

    def test_get_conditions_and_values(self):
        kwargs = {
            'a': None,
            'b': 1,
            'c': 'three',
        }
        conditions, values = TweetRepository._get_conditions_and_values(
            **kwargs)
        expected_conditions = 'b = %s AND c = %s'
        self.assertEqual(expected_conditions, conditions)
        expected_values = [1, 'three']
        self.assertEqual(expected_values, values)

    def test_get_selector_returns_star_if_no_projection(self):
        kwargs = {}
        selector = TweetRepository._get_selector(**kwargs)
        expected = '*'
        self.assertEqual(expected, selector)

    def test_get_selector_returns_projection_if_specified(self):
        kwargs = {'projection': ['attr1', 'attr2']}
        selector = TweetRepository._get_selector(**kwargs)
        expected = 'attr1,attr2'
        self.assertEqual(expected, selector)
