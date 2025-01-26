from datetime import datetime

def add_processing_timestamp(data):
    """Generate a UTC processing timestamp."""
    data['processing_timestamp'] = datetime.utcnow().isoformat()

    return data



