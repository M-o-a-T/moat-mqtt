def get_version():
    import pkg_resources

    return pkg_resources.get_distribution("distmqtt").version
