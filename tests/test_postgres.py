from datetime import datetime
import unittest

from psycopg2.errors import UniqueViolation

from tests.implementations import create_test_database, TweetPgsqlRepository, \
    TweetStatsRepository


class TestItemToInsertStatement(unittest.TestCase):

    def test_trouble_case(self):
        repo = TweetStatsRepository()
        item = {
            'tweet_id': 'Ugx7w7FYrLQILWq-TE14AaABAg',
            'collected_at': datetime(2021, 11, 29, 3, 13, 53, 809005),
            'num_likes': 0}
        sql, _ = repo._item_to_insert_statement(item, upsert=True)
        self.assertIn('SET num_likes = ', sql)


class TestPostgresRepository(unittest.TestCase):

    def test_get_conditions_and_values(self):
        kwargs = {
            'a': None,
            'b': 1,
            'c': 'three',
        }
        conditions, values = TweetPgsqlRepository._get_conditions_and_values(
            **kwargs)
        expected_conditions = 'b = %s, c = %s'
        self.assertEqual(expected_conditions, conditions)
        expected_values = [1, 'three']
        self.assertEqual(expected_values, values)

    def test_get_selector_returns_star_if_no_projection(self):
        kwargs = {}
        selector = TweetPgsqlRepository._get_selector(**kwargs)
        expected = '*'
        self.assertEqual(expected, selector)

    def test_get_selector_returns_star_if_projection_is_none(self):
        kwargs = {'projection': None}
        selector = TweetPgsqlRepository._get_selector(**kwargs)
        expected = '*'
        self.assertEqual(expected, selector)

    def test_get_selector_returns_projection_if_specified(self):
        kwargs = {'projection': ['attr1', 'attr2']}
        selector = TweetPgsqlRepository._get_selector(**kwargs)
        expected = 'attr1,attr2'
        self.assertEqual(expected, selector)

    def test_add_inserts_item(self):
        db_name = 'test_add_inserts_item'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet = {'label': None, 'tweet_id': 1, 'tweet': 'tweet1'}
        repo.add(tweet)
        item = repo.get(1)
        self.assertEqual(tweet, item)

    def test_add_ignores_duplicates_when_requested(self):
        db_name = 'test_add_ignores_duplicates_when_requested'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet = {'tweet_id': 1, 'tweet': 'tweet1'}
        repo.add(tweet)
        error = False
        try:
            repo.add(tweet, ignore_duplicates=True)
        except UniqueViolation:
            error = True
        self.assertFalse(error)

    def test_add_errors_duplicates_when_requested(self):
        db_name = 'test_add_errors_duplicates_when_requested'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet = {'tweet_id': 1, 'tweet': 'tweet1'}
        repo.add(tweet)
        error = False
        try:
            repo.add(tweet, ignore_duplicates=False)
        except UniqueViolation:
            error = True
        self.assertTrue(error)

    def test_add_many(self):
        db_name = 'test_add_many'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        tweet2 = {'tweet_id': 2, 'tweet': 'tweet2'}
        repo.add_many([tweet1, tweet2])
        self.assertTrue(repo.exists(1))
        self.assertTrue(repo.exists(2))

    def test_add_many_rolls_back_on_error(self):
        db_name = 'test_add_many_rolls_back_on_error'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        tweet2 = {'tweet_id': 1, 'tweet': 'tweet1'}
        try:
            repo.add_many([tweet1, tweet2])
        except UniqueViolation:
            pass
        self.assertFalse(repo.exists(1))

    def test_all_returns_all_items(self):
        db_name = 'test_all_returns_all_items'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'label': None, 'tweet_id': 1, 'tweet': 'tweet1'}
        tweet2 = {'label': None, 'tweet_id': 2, 'tweet': 'tweet2'}
        repo.add(tweet1)
        repo.add(tweet2)
        items = list(repo.all())
        expected = [tweet1, tweet2]
        self.assertEqual(expected, items)

    def test_count_returns_number_of_items_in_table(self):
        db_name = 'test_count_returns_number_of_items_in_table'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        tweet2 = {'tweet_id': 2, 'tweet': 'tweet2'}
        tweet3 = {'tweet_id': 3, 'tweet': 'tweet3'}
        repo.add(tweet1)
        repo.add(tweet2)
        repo.add(tweet3)
        count = repo.count()
        self.assertEqual(3, count)

    def test_delete_removes_items(self):
        db_name = 'test_delete_removes_items'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        repo.add(tweet1)
        self.assertTrue(repo.exists(1))
        repo.delete(dict(tweet_id=1))
        self.assertFalse(repo.exists(1))

    def test_delete_many(self):
        db_name = 'test_delete_many'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        tweet2 = {'tweet_id': 2, 'tweet': 'tweet2'}
        repo.add_many([tweet1, tweet2])
        self.assertTrue(repo.exists(1))
        self.assertTrue(repo.exists(2))
        repo.delete_many([{'tweet_id': 1}, {'tweet_id': 2}])
        self.assertFalse(repo.exists(1))
        self.assertFalse(repo.exists(2))

    def test_exists_returns_true_when_item_exists(self):
        db_name = 'test_exists_returns_true_when_item_exists'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        repo.add(tweet1)
        self.assertTrue(repo.exists(1))

    def test_exists_returns_false_when_item_does_not_exist(self):
        db_name = 'test_exists_returns_false_when_item_does_not_exist'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        self.assertFalse(repo.exists(1))

    def test_get_returns_none_when_item_does_not_exist(self):
        db_name = 'test_get_returns_none_when_item_does_not_exist'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        item = repo.get(1)
        self.assertIsNone(item)

    def test_get_returns_item_when_exists(self):
        db_name = 'test_get_returns_item_when_exists'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        repo.add(tweet1)
        item = repo.get(1)
        self.assertIsInstance(item, dict)
        self.assertIsNotNone(item)

    def test_search_returns_correct_items(self):
        db_name = 'test_search_returns_correct_items'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1'}
        tweet2 = {'tweet_id': 2, 'tweet': 'tweet1'}
        tweet3 = {'tweet_id': 3, 'tweet': 'tweet3'}
        repo.add(tweet1)
        repo.add(tweet2)
        repo.add(tweet3)
        items = list(repo.search(tweet='tweet1'))
        self.assertEqual(2, len(items))

    def test_update_single_item(self):
        db_name = 'test_update_single_item'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet = {'tweet_id': 1, 'tweet': 'tweet1', 'label': 'a'}
        repo.add(tweet)
        result = repo.get(1)
        self.assertEqual('a', result['label'])
        tweet['tweet'] = 'edited tweet'
        tweet['label'] = 'b'
        repo.update(tweet, ['tweet_id'], ['tweet', 'label'])
        result = repo.get(1)
        self.assertEqual('edited tweet', result['tweet'])
        self.assertEqual('b', result['label'])

    def test_update_many_items(self):
        db_name = 'test_update_many_items'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1', 'label': 'a'}
        tweet2 = {'tweet_id': 2, 'tweet': 'tweet2', 'label': 'a'}
        repo.add(tweet1)
        repo.add(tweet2)
        result = repo.get(1)
        self.assertEqual('a', result['label'])
        result = repo.get(2)
        self.assertEqual('a', result['label'])
        tweet1['label'] = 'b'
        tweet2['label'] = 'b'
        repo.update_many([tweet1, tweet2], ['tweet_id'], ['label'])
        result = repo.get(1)
        self.assertEqual('b', result['label'])
        result = repo.get(2)
        self.assertEqual('b', result['label'])

    def test_upsert_ok_when_not_exists(self):
        db_name = 'test_upsert_ok_when_not_exists'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet = {'tweet_id': 1, 'tweet': 'tweet1', 'label': 'a'}
        repo.upsert(tweet)
        result = repo.get(1)
        self.assertEqual(tweet, result)

    def test_upsert_ok_when_exists(self):
        db_name = 'test_upsert_ok_when_exists'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet = {'tweet_id': 1, 'tweet': 'tweet1', 'label': 'a'}
        repo.upsert(tweet)
        result = repo.get(1)
        self.assertEqual(tweet, result)
        error = False
        try:
            repo.upsert(tweet)
        except UniqueViolation:
            error = True
        self.assertFalse(error)
        result = repo.get(1)
        self.assertEqual(tweet, result)

    def test_upsert_many(self):
        db_name = 'test_upsert_many'
        create_test_database(db_name)
        repo = TweetPgsqlRepository(db_name=db_name)
        tweet1 = {'tweet_id': 1, 'tweet': 'tweet1', 'label': 'a'}
        repo.add(tweet1)

        tweet1['label'] = 'b'
        tweet2 = {'tweet_id': 2, 'tweet': 'tweet2', 'label': 'b'}
        repo.upsert_many([tweet1, tweet2])

        result = repo.get(1)
        self.assertEqual('b', result['label'])
        result = repo.get(2)
        self.assertEqual('b', result['label'])

    def test_upsert_with_two_primary_keys(self):
        db_name = 'test_upsert_with_two_primary_keys'
        create_test_database(db_name)
        repo = TweetStatsRepository(db_name=db_name)
        now = datetime.now()
        tweet_stats = {
            'tweet_id': 1,
            'collected_at': now,
            'num_likes': 1}
        repo.upsert(tweet_stats)
        result = repo.get(1, now)
        self.assertEqual(tweet_stats, result)
