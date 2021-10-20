import unittest

from pymongo.errors import DuplicateKeyError

from tests.implementations import TweetMongoRepository


class TestMongoRepository(unittest.TestCase):

    def test_add(self):
        repo = TweetMongoRepository('test_add')
        tweet = {'id': 1, 'text': 'tweet1'}
        repo.add(tweet)
        tweet = repo.get(1)
        expected = {'_id': 1, 'id': 1, 'text': 'tweet1'}
        self.assertEqual(expected, tweet)

    def test_add_errors_when_duplicate_disallowed(self):
        repo = TweetMongoRepository('test_add_errors_when_duplicate_disallowed')
        tweet = {'id': 1, 'text': 'tweet1'}
        repo.add(tweet)
        error = False
        try:
            repo.add(tweet, error_duplicates=True)
        except DuplicateKeyError:
            error = True
        self.assertTrue(error)

    def test_add_does_not_error_when_duplicate_ignored(self):
        repo = TweetMongoRepository(
            'test_add_does_not_error_when_duplicate_ignored')
        tweet = {'id': 1, 'text': 'tweet1'}
        repo.add(tweet)
        error = False
        try:
            repo.add(tweet, error_duplicates=False)
        except DuplicateKeyError:
            error = True
        self.assertFalse(error)

    def test_all(self):
        repo = TweetMongoRepository('test_all')
        repo.add({'id': 1, 'text': 'tweet1'})
        repo.add({'id': 2, 'text': 'tweet2'})
        tweets = list(repo.all())
        self.assertEqual(2, len(tweets))
        ids = set([x['_id'] for x in tweets])
        expected = {1, 2}
        self.assertSetEqual(expected, ids)

    def test_count(self):
        repo = TweetMongoRepository('test_count')
        repo.add({'id': 1, 'text': 'tweet1'})
        repo.add({'id': 2, 'text': 'tweet2'})
        repo.add({'id': 3, 'text': 'tweet3'})
        count = repo.count()
        self.assertEqual(3, count)

    def test_delete(self):
        repo = TweetMongoRepository('test_delete')
        tweet = {'id': 1, 'text': 'tweet1'}
        repo.add(tweet)
        tweet = repo.get(1)
        expected = {'_id': 1, 'id': 1, 'text': 'tweet1'}
        self.assertEqual(expected, tweet)
        repo.delete(1)
        tweet = repo.get(1)
        self.assertIsNone(tweet)

    def test_exists_returns_false_when_not_exists(self):
        repo = TweetMongoRepository('test_exists_returns_false_when_not_exists')
        exists = repo.exists(_id=1)
        self.assertFalse(exists)

    def test_exists_returns_true_when_exists(self):
        repo = TweetMongoRepository('test_exists_returns_true_when_exists')
        tweet = {'id': 1, 'text': 'tweet1'}
        repo.add(tweet)
        exists = repo.exists(_id=1)
        self.assertTrue(exists)

    def test_get_returns_tweet_when_exists(self):
        repo = TweetMongoRepository('test_get_returns_tweet_when_exists')
        tweet = {'id': 1, 'text': 'tweet1'}
        repo.add(tweet)
        tweet = repo.get(1)
        self.assertIsNotNone(tweet)
        expected = {'_id': 1, 'id': 1, 'text': 'tweet1'}
        self.assertEqual(expected, tweet)

    def test_get_returns_none_when_not_exists(self):
        repo = TweetMongoRepository('test_get_returns_none_when_not_exists')
        tweet = repo.get(1)
        self.assertIsNone(tweet)

    def test_search(self):
        repo = TweetMongoRepository('test_search')
        repo.add({'id': 1, 'text': 'tweet1', 'label': 'a'})
        repo.add({'id': 2, 'text': 'tweet2', 'label': 'a'})
        repo.add({'id': 3, 'text': 'tweet3', 'label': 'b'})
        tweets = list(repo.search(label='a'))
        self.assertEqual(2, len(tweets))
        ids = set([x['id'] for x in tweets])
        expected = {1, 2}
        self.assertSetEqual(expected, ids)
