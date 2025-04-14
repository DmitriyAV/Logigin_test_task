def analyze_events(events):
    event_counts = Counter(int)
    event_durations = defaultdict(list)
    ip_counts = defaultdict(int)
    failed_events = []
    slow_events = []