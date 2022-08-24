from datetime import datetime
import kong_pdk.pdk.kong as kong
import token_bucket
import functools

Schema = [
  {"HEADER_NAME": {"type": "string"}},
]
version = "0.1.0"
priority = 1000


class Plugin:
    # Load values from artifact
    permissions_mapping = {
      "PERMISSION_1": "1",
      "PERMISSION_2": "2",
      "PERMISSION_3": "3"
    }

    def __init__(self, config):
        self.config = config
        self.storage = token_bucket.MemoryStorage()
        self.limiter = token_bucket.Limiter(1,  # Number of tokens per second to add to the bucket
                                            5,  # Maximum number of tokens that the bucket can hold.
                                                # Once the bucket is full, additional tokens are discarded.
                                            self.storage)

    def access(self, kong: kong.kong):
        header_name = self.config["HEADER_NAME"]
        header_value = kong.request.get_header(header_name)
        consume_one = functools.partial(self.limiter.consume, 'key')
        consume_one()
        token_count = self.storage.get_token_count('key')
        if header_value is None:
            kong.response.set_header("token_count", str(token_count))
            kong.service.request.set_header("token_count", str(token_count))
            return kong.response.error(400, f"Missing header: {header_name}, "
                                            f"token_count: {token_count}")
        else:
            kong.service.request.set_header("now", str(datetime.utcnow()))
            kong.service.request.set_header("token_count", str(token_count))
            kong.service.request.clear_header(header_name)
            kong.service.request.set_header("x-permissions", self.encode_permissions(header_value))
        if token_count < 1:
            kong.response.set_header("token_count", str(token_count))
            return kong.response.error(429, "Reached rate limit, "
                                            "num_tokens must be >= 1, "
                                            f"token_count: {token_count}")

    def encode_permissions(self, permission_header):
        # Custom logic
        return self.permissions_mapping[permission_header]
