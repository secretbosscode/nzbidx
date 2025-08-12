from nzbidx_api import main


def test_build_index_template_without_ilm():
    template = main.build_index_template(ilm=False)
    settings = template["template"]["settings"]
    assert "index.lifecycle.name" not in settings
    assert "index.lifecycle.rollover_alias" not in settings


def test_init_opensearch_without_ilm(monkeypatch):
    body_holder = {}

    class DummyIndices:
        def put_index_template(self, *, name, body):
            body_holder["body"] = body

        def get_alias(self, name):
            raise Exception()

        def exists(self, index):
            return False

        def create(self, *, index, body):
            pass

        def put_alias(self, **kwargs):
            pass

    class DummyClient:
        def __init__(self, *args, **kwargs):
            self.indices = DummyIndices()

        def close(self):
            pass

    monkeypatch.setattr(main, "OpenSearch", DummyClient)
    monkeypatch.setenv("OPENSEARCH_URL", "http://example.com")
    main.opensearch = None
    main.init_opensearch()
    assert isinstance(main.opensearch, DummyClient)
    settings = body_holder["body"]["template"]["settings"]
    assert "index.lifecycle.name" not in settings
