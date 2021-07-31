from hashlib import sha256
import secrets

from module.decopy.utils.Constants import salt


def get_random_str(bit: int):
    num = [chr(i) for i in range(48, 58)]
    char = [chr(i) for i in range(97, 123)]
    total = num + char
    return "".join(secrets.SystemRandom().sample(total,bit))


def pseudonymize(field):
    return sha256(field.encode() + salt.encode()).hexdigest()[:16]