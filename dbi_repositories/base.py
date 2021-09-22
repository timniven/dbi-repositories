from typing import List, Optional


class Repository:
    """Abstract base Repository class."""

    def __init__(self, **kwargs):
        self._kwargs = kwargs

    def __enter__(self):
        self.__init__(**self._kwargs)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # implementation note - see: https://stackoverflow.com/questions
        # /22417323/how-do-enter-and-exit-work-in-python-decorator-classes
        self.dispose()

    def add(self, *args, **kwargs):
        """Add one item to the table/collection."""
        raise NotImplementedError

    def all(self, projection: Optional[List[str]] = None):
        """Get all records in the table/collection.

        Args:
          projection: List, optional, of attributes to project.
        """
        raise NotImplementedError

    def commit(self):
        """Save changes to the database."""
        raise NotImplementedError

    def count(self) -> int:
        """Get a count of how many records are in this table/collection."""
        raise NotImplementedError

    def delete(self, *args, **kwargs):
        """Delete an item from the table/collection."""
        raise NotImplementedError

    def dispose(self):
        """Dispose of this Repository."""
        raise NotImplementedError

    def exists(self, *args, **kwargs) -> bool:
        """Check if an item exists in the table/collection."""
        raise NotImplementedError

    def get(self, projection: Optional[List[str]] = None, *args, **kwargs):
        """Get an item from the table/collection."""
        raise NotImplementedError

    def search(self, projection: Optional[List[str]] = None, *args, **kwargs):
        """Search for records in the table/collection."""
        raise NotImplementedError
