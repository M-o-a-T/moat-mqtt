class BaseTopicPlugin:
    def __init__(self, context):
        self.context = context
        try:
            self.topic_config = self.context.config["topic-check"]
        except KeyError:
            self.context.logger.warning("'topic-check' section not found in context configuration")
            self.topic_config = None

    async def topic_filtering(self, *args, **kwargs):  # pylint: disable=unused-argument
        if not self.topic_config:
            # auth config section not found
            return None
        return True


class TopicTabooPlugin(BaseTopicPlugin):
    def __init__(self, context):
        super().__init__(context)
        self._taboo = ["prohibited", "top-secret", "data/classified"]

    async def topic_filtering(self, *args, **kwargs):
        filter_result = await super().topic_filtering(*args, **kwargs)
        if filter_result:
            session = kwargs.get("session", None)
            topic = kwargs.get("topic", None)
            if session.username and session.username == "admin":
                return True
            if topic and topic in self._taboo:
                return False
            return True
        return filter_result


class TopicAccessControlListPlugin(BaseTopicPlugin):
    @staticmethod
    def topic_ac(topic_requested, topic_allowed):
        req_split = topic_requested.split("/")
        allowed_split = topic_allowed.split("/")
        ret = True
        for i in range(max(len(req_split), len(allowed_split))):
            try:
                a_aux = req_split[i]
                b_aux = allowed_split[i]
            except IndexError:
                ret = False
                break
            if b_aux == "#":
                break
            elif (b_aux == "+") or (b_aux == a_aux):
                continue
            else:
                ret = False
                break
        return ret

    async def topic_filtering(self, *args, **kwargs):
        if not self.topic_config:
            # auth config section not found
            return None
        filter_result = await super().topic_filtering(*args, **kwargs)
        if filter_result:
            session = kwargs.get("session", None)
            req_topic = kwargs.get("topic", None)
            if req_topic:
                username = session.username
                if username is None:
                    username = "anonymous"
                try:
                    allowed_topics = self.topic_config["acl"].get(username, [])
                except KeyError:
                    return True
                else:
                    for allowed_topic in allowed_topics:
                        if self.topic_ac(req_topic, allowed_topic):
                            return True
                    return False
            else:
                return False
