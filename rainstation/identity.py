def model_for_device(spec, device):
    return spec.get('device_models', {}).get(device)
