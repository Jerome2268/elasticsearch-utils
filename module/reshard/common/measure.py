def convert(store_size: str):
    # compute the size
    if store_size == "none":
        return 0.0
    if store_size is None:
        return 0.0
    dorr_store = 0.0
    if "k" in store_size:
        dorr_store += float(store_size[:-2])
    elif "m" in store_size:
        dorr_store += float(store_size[:-2]) * 1024
    elif "g" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024
    elif "t" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024
    elif "p" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024
    elif "e" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024
    elif "z" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
    elif "y" in store_size:
        dorr_store += float(store_size[:-2]) * 1024 * 1024 * 1024 * 1024 * 1024 * 1024 * 1024
    else:
        # byte
        dorr_store += float(store_size[:-1]) / 1024
    return dorr_store
