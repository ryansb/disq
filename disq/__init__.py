import pkg_resources

from disq.client import DisqueAlpha

Disque = DisqueAlpha

__all__ = ['DisqueAlpha', 'Disque']

__version__ = pkg_resources.get_distribution('disq').version
