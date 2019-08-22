import re
import ipaddress
import six

def dict_contains_path(dct, keys):
    """
    Tests whether the keys exist recursively in `dictionary`.

    :type dct: dict
    :type keys: list
    :rtype: bool
    """
    if keys:
        if not isinstance(dct, dict):
            return False
        key = keys.pop(0)
        if key in dct:
            dct = dct[key]
            return dict_contains_path(dct, keys)
        return False
    return True

def _parse_addr(value):
    """
    Get the IP address the RGW is running on.

    >>> _parse_addr('192.168.178.3:49774/1534999298')
    '192.168.178.3'

    >>> _parse_addr('[2001:db8:85a3::8a2e:370:7334]:49774/1534999298')
    '2001:db8:85a3::8a2e:370:7334'

    >>> _parse_addr('xyz')
    Traceback (most recent call last):
    ...
    LookupError: Failed to determine RGW address

    >>> _parse_addr('192.168.178.a:8080/123456789')
    Traceback (most recent call last):
    ...
    LookupError: Invalid RGW address '192.168.178.a' found

    >>> _parse_addr('[2001:0db8:1234]:443/123456789')
    Traceback (most recent call last):
    ...
    LookupError: Invalid RGW address '2001:0db8:1234' found

    >>> _parse_addr('2001:0db8::1234:49774/1534999298')
    Traceback (most recent call last):
    ...
    LookupError: Failed to determine RGW address

    :param value: The string to process. The syntax is '<HOST>:<PORT>/<NONCE>'.
    :type: str
    :raises LookupError if parsing fails to determine the IP address.
    :return: The IP address.
    :rtype: str
    """
    match = re.search(r'^(\[)?(?(1)([^\]]+)\]|([^:]+)):\d+/\d+?', value)
    if match:
        # IPv4:
        #   Group 0: 192.168.178.3:49774/1534999298
        #   Group 3: 192.168.178.3
        # IPv6:
        #   Group 0: [2001:db8:85a3::8a2e:370:7334]:49774/1534999298
        #   Group 1: [
        #   Group 2: 2001:db8:85a3::8a2e:370:7334
        addr = match.group(3) if match.group(3) else match.group(2)
        try:
            ipaddress.ip_address(six.u(addr))
            return addr
        except ValueError:
            raise LookupError('Invalid RGW address \'{}\' found'.format(addr))
    raise LookupError('Failed to determine RGW address')


def _parse_frontend_config(config):
    """
    Get the port the RGW is running on. Due the complexity of the
    syntax not all variations are supported.

    Get more details about the configuration syntax here:
    http://docs.ceph.com/docs/master/radosgw/frontends/
    https://civetweb.github.io/civetweb/UserManual.html

    >>> _parse_frontend_config('beast port=8000')
    (8000, False)

    >>> _parse_frontend_config('civetweb port=8000s')
    (8000, True)

    >>> _parse_frontend_config('beast port=192.0.2.3:80')
    (80, False)

    >>> _parse_frontend_config('civetweb port=172.5.2.51:8080s')
    (8080, True)

    >>> _parse_frontend_config('civetweb port=[::]:8080')
    (8080, False)

    >>> _parse_frontend_config('civetweb port=ip6-localhost:80s')
    (80, True)

    >>> _parse_frontend_config('civetweb port=[2001:0db8::1234]:80')
    (80, False)

    >>> _parse_frontend_config('civetweb port=[::1]:8443s')
    (8443, True)

    >>> _parse_frontend_config('civetweb port=xyz')
    Traceback (most recent call last):
    ...
    LookupError: Failed to determine RGW port

    >>> _parse_frontend_config('civetweb')
    Traceback (most recent call last):
    ...
    LookupError: Failed to determine RGW port

    :param config: The configuration string to parse.
    :type config: str
    :raises LookupError if parsing fails to determine the port.
    :return: A tuple containing the port number and the information
             whether SSL is used.
    :rtype: (int, boolean)
    """
    match = re.search(r'port=(.*:)?(\d+)(s)?', config)
    if match:
        port = int(match.group(2))
        ssl = match.group(3) == 's'
        return port, ssl
    raise LookupError('Failed to determine RGW port')


