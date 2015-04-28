import pkg_resources

from disque.client import DisqueAlpha

Disque = DisqueAlpha

__all__ = ['DisqueAlpha', 'Disque']

__version__ = pkg_resources.get_distribution('disque').version
