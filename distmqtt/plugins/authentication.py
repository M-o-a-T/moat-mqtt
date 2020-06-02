# Copyright (c) 2015 Nicolas JOUANIN
#
# See the file license.txt for copying permission.

from passlib.apps import custom_app_context as pwd_context


class BaseAuthPlugin:
    def __init__(self, context):
        self.context = context
        try:
            self.auth_config = self.context.config["auth"]
        except KeyError:
            self.context.logger.warning(
                "'auth' section not found in context configuration"
            )

    def authenticate(self, *args, **kwargs):  # pylint: disable=unused-argument
        if not self.auth_config:
            # auth config section not found
            self.context.logger.warning(
                "'auth' section not found in context configuration"
            )
            return False
        return True


class AnonymousAuthPlugin(BaseAuthPlugin):
    async def authenticate(self, *args, **kwargs):
        authenticated = super().authenticate(*args, **kwargs)
        if authenticated:
            allow_anonymous = self.auth_config.get(
                "allow-anonymous", True
            )  # allow anonymous by default
            if allow_anonymous:
                authenticated = True
                self.context.logger.debug(
                    "Authentication success: config allows anonymous"
                )
            else:
                try:
                    session = kwargs["session"]
                except KeyError:
                    self.context.logger.warning("Session informations not available")
                    authenticated = False
                else:
                    authenticated = True if session and session.username else False
                    if authenticated:
                        self.context.logger.debug(
                            "Authentication success: user %r", session.username
                        )
                    else:
                        self.context.logger.debug(
                            "Authentication failure: session has an empty username"
                        )
        return authenticated


class FileAuthPlugin(BaseAuthPlugin):
    def __init__(self, context):
        super().__init__(context)
        self._users = dict()
        self._read_password_file()

    def _read_password_file(self):
        password_file = self.auth_config.get("password-file", None)
        if password_file:
            with open(password_file) as f:
                for l in f:
                    line = l.strip()
                    if not line.startswith("#"):  # Allow comments in files
                        username, pwd_hash = line.split(sep=":", maxsplit=1)
                        if username:
                            self._users[username] = pwd_hash
            self.context.logger.debug(
                "%d user(s) read from file '%s'", len(self._users), password_file
            )
        else:
            self.context.logger.warning(
                "Configuration parameter 'password_file' not found"
            )

    async def authenticate(self, *args, **kwargs):
        authenticated = super().authenticate(*args, **kwargs)
        if authenticated:
            session = kwargs.get("session", None)
            if session and session.username:
                name = self._users.get(session.username, None)
                if not name:
                    authenticated = False
                    self.context.logger.debug(
                        "No hash found for user '%s'", session.username
                    )
                else:
                    authenticated = pwd_context.verify(session.password, name)
            else:
                return None
        return authenticated
