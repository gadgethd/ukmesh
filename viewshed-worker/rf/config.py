import math
import os
import time

ANTENNA_HEIGHT_M = 5
COVERAGE_TARGET_HEIGHT_M = 5
LINK_LOS_MAX_V = float(os.environ.get('LINK_LOS_MAX_V', '1.12'))

K_FACTOR = 4 / 3
R_EARTH_M = 6_371_000

FREQ_MHZ = 868.0
LAMBDA_M = 3e8 / (FREQ_MHZ * 1e6)
PROFILE_STEP_M = 250.0

DEFAULT_USABLE_PATH_LOSS_DB = float(os.environ.get('DEFAULT_USABLE_PATH_LOSS_DB', '137.5'))
CALIBRATION_REFRESH_S = int(os.environ.get('COVERAGE_CALIBRATION_REFRESH_S', '900'))
CALIBRATION_MIN_LINKS = int(os.environ.get('COVERAGE_CALIBRATION_MIN_LINKS', '24'))
CALIBRATION_MIN_OBSERVED_COUNT = int(os.environ.get('COVERAGE_CALIBRATION_MIN_OBSERVED_COUNT', '3'))
CALIBRATION_MAX_THRESHOLD_BOOST_DB = float(os.environ.get('COVERAGE_CALIBRATION_MAX_THRESHOLD_BOOST_DB', '2'))
CALIBRATION_PERCENTILE = float(os.environ.get('COVERAGE_CALIBRATION_PERCENTILE', '0.9'))
CALIBRATION_EXTRA_MARGIN_DB = float(os.environ.get('COVERAGE_CALIBRATION_EXTRA_MARGIN_DB', '0.5'))

RF_CALIBRATION = {
    'usable_path_loss_db': DEFAULT_USABLE_PATH_LOSS_DB,
    'signal_thresholds_db': {
        'green': max(116.0, DEFAULT_USABLE_PATH_LOSS_DB - 16.0),
        'amber': max(124.0, DEFAULT_USABLE_PATH_LOSS_DB - 8.0),
        'red': DEFAULT_USABLE_PATH_LOSS_DB,
    },
    'samples': 0,
    'updated_at': 0.0,
}

RADIO_NEIGHBOR_SYNC = {
    'updated_at': 0.0,
}

def current_usable_path_loss_db() -> float:
    return float(RF_CALIBRATION['usable_path_loss_db'])

def current_signal_thresholds_db() -> dict[str, float]:
    return dict(RF_CALIBRATION['signal_thresholds_db'])

def radio_snr_band(snr_db):
    if snr_db is None or not math.isfinite(float(snr_db)):
        return 'unknown'
    snr = float(snr_db)
    if snr >= 8.0:
        return 'strong'
    if snr >= 2.0:
        return 'medium'
    if snr >= 0.0:
        return 'weak'
    return 'poor'
