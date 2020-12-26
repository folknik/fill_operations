import math


EARTH_RADIUS = 6371008.7714  # meter


def distance_between(lat1, lon1, lat2, lon2):
    lambda1 = math.radians(lon1)
    lambda2 = math.radians(lon2)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = phi2 - phi1
    dlambda = lambda2 - lambda1
    a = math.pow(math.sin(dphi / 2), 2) + math.cos(phi1) * math.cos(phi2) * math.pow(math.sin(dlambda / 2), 2)
    dsigma = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return EARTH_RADIUS * dsigma


def calculate_angle(lat1, lon1, lat2, lon2):
    lambda1 = (lon1 * math.pi) / 180
    lambda2 = (lon2 * math.pi) / 180
    fi1 = (lat1 * math.pi) / 180
    fi2 = (lat2 * math.pi) / 180
    y = math.sin(lambda2 - lambda1) * math.cos(fi2)
    x = math.cos(fi1) * math.sin(fi2) - math.sin(fi1) * math.cos(fi2) * math.cos(lambda2 - lambda1)
    angle = ((math.atan2(y, x) * 180) / math.pi + 360) % 360
    return angle


def most_common(lst):
    return max(set(lst), key=lst.count)