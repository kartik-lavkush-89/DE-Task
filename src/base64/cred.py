import os
import base64
from dotenv import load_dotenv

load_dotenv()

cred = os.getenv('CREDENTIALS')
cred_string_bytes = cred.encode("ascii")
base64_bytes = base64.b64encode(cred_string_bytes)
base64_string = base64_bytes.decode("ascii")