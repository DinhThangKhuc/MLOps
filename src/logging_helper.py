import os
import logging

from dotenv import load_dotenv
load_dotenv()

level = logging.DEBUG if bool(os.getenv("DEBUG")) else logging.ERROR

logging.basicConfig(format='%(levelname)s: %(asctime)s [%(filename)s.%(funcName)s]: %(message)s',
                    level=level)

logger = logging.getLogger(__name__)

