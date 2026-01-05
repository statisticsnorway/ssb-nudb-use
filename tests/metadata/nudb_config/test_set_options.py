from nudb_use.metadata.nudb_config.set_options import set_option


def test_set_option():
    settings = set_option("warn_unsafe_derive", False)
    assert not settings.options.warn_unsafe_derive
    assert not settings.options["warn_unsafe_derive"]
