"""One fixed paired-read policy; workload, collector and report share it."""
from pathlib import Path
from common import digest, read_json

PATH = Path(__file__).with_suffix('.json')
PROFILE = read_json(PATH)
IDENTITY = {'version': PROFILE['version'], 'sha256': digest(PATH)}
STEPS = [{'pair': pair + 1, 'position': pair * 2 + index, 'side': side}
         for pair, sides in enumerate(PROFILE['pair_order']) for index, side in enumerate(sides)]
