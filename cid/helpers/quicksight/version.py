""" Semantic version of dashboards in CID
"""

import re
import logging

logger = logging.getLogger(__name__)

# Default version used when version cannot be determined
DEFAULT_VERSION = "v0.0.0"

class CidVersion:
    """ Semantic version of dashboards in CID
    """

    def __init__(self, version):

        if isinstance(version, __class__):
            self.major, self.minor, self.build = version.major, version.minor, version.build
        elif isinstance(version, str):
            self.major, self.minor, self.build = self._parse(version)
        else:
            raise TypeError(f'{version} must be {__class__} or str ')

    def __str__(self):
        return f'v{self.major}.{self.minor}.{self.build}'

    def _parse(self, str_version, default=(1, 0, 0)):
        version_pattern = re.compile(r"^[v|V](?P<major>[0-9]+)\.(?P<minor>[0-9]+)(\.(?P<build>[0-9]+))?$")
        results = version_pattern.match(str_version)

        if not results:
            logger.debug(f'Could not find version pattern in provided string: {str_version} will use default ({default})')
            major, minor, build = default
        else:
            major = int(results.group("major"))
            minor = int(results.group("minor") or 0)
            build = int(results.group("build") or 0)
        return major, minor, build

    def compatible_versions(self, _version) -> bool:
        """ Return True when both version are on the same major branch """
        return CidVersion(_version).major == self.major

    def __lt__(self, _version):
        return self.as_tuple() < CidVersion(_version).as_tuple()

    def __le__(self, _version):
        return self.as_tuple() <= CidVersion(_version).as_tuple()

    def __eq__(self, _version):
        return self.as_tuple() == CidVersion(_version).as_tuple()

    def __ge__(self, _version):
        return self.as_tuple() >= CidVersion(_version).as_tuple()

    def __gt__(self, _version):
        return self.as_tuple() > CidVersion(_version).as_tuple()

    def __ne__(self, _version):
        return self.as_tuple() != CidVersion(_version).as_tuple()

    def as_tuple(self) -> tuple:
        """ return version as tuple """
        return (self.major, self.minor, self.build)


def test_versions():
    """ basic tests for versions
    """
    assert CidVersion('v1.2').as_tuple() == (1, 2, 0)
    assert CidVersion('v1.2.3').as_tuple() == (1, 2, 3)
    assert CidVersion('v1.3.3') > CidVersion('V1.2.4')
    assert CidVersion('v1.3.3') >= CidVersion('v1.3.3')
    assert CidVersion(CidVersion('v1.2')).as_tuple() == (1, 2, 0)
    assert str(CidVersion('v1.2')) == 'v1.2.0'


def test_version_raises():
    """ test exception cases
    """
    import pytest
    with pytest.raises(TypeError):
         CidVersion(1)