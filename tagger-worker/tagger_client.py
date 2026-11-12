"""TypeSafe HTTP client and response normalization."""

import json

import time

import urllib.request


QUESTIONS = {
    "speaker": {
        "type": "choice",
        "instructions": "Who most likely sent this message?",
        "criteria": {
            "human": "A human operator typing a message",
            "bot": "An automated bot or script",
            "room-server": "A room server, repeater, or bridge auto-response",
            "unclear": "Cannot tell from the message",
        },
    },
    "kind": {
        "type": "choice",
        "instructions": "What is the primary kind of this message?",
        "criteria": {
            "chat": "Social conversation, greeting, banter, or sharing (links, pictures, files, images)",
            "radio-check": "Checking whether/how their signal is received",
            "test-ping": "A test or ping",
            "ack": "Acknowledgement of a previous signal",
            "position": "Sharing their whereabouts or movements (e.g. 'I am near X', 'heading to Y', 'going off air for a bit') - not announcements about infrastructure or network status",
            "coverage": "Reporting range, hops, or propagation",
            "help": "Asking for help or information from others",
            "noise": "No meaningful content (gibberish, stray characters, emoji only)",
            "spam": "Only unsolicited promotion, scams, or abuse. IMPORTANT: a bare link, picture, or file with no promotional text is NEVER spam (mesh users share links/images routinely); when choosing between spam and chat for a bare link, choose chat",
            "automation": "Automated telemetry, home-automation, or sensor notification",
            "command": "A command to a system or bot (e.g. !test, !path, !help)",
        },
    },
    "topic": {
        "type": "choice",
        "instructions": "What is this message mainly about?",
        "criteria": {
            "weather": "Weather or conditions",
            "meetups": "Meetups, events, or social plans",
            "travel": "Travel, journeys, or being on the move",
            "gear": "Radios, antennas, equipment, or tech gear",
            "network": "Network coverage, repeaters, hops, or connectivity",
            "help": "Getting help, support, or answering questions",
            "humour": "Jokes, banter, or humour",
            "general": "General chatter that fits nothing else",
        },
    },
    "mentions_location": {
        "type": "noul",
        "instructions": "Does the message mention a geographic place (town, area, or landmark)?",
    },
    "directed": {
        "type": "noul",
        "instructions": "Is the message addressed to a specific person or node (by name or @mention)?",
    },
    "safety": {
        "type": "noul",
        "instructions": "Is this message safety or emergency related?",
    },
}


def classify(state, settings):
    payload = {"state": state, "model": settings.model, "questions": QUESTIONS}
    req = urllib.request.Request(
        settings.api_url,
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"Bearer {settings.api_key}",
            "Content-Type": "application/json",
        },
    )
    t0 = time.time()
    with urllib.request.urlopen(req, timeout=90) as r:
        resp = json.loads(r.read().decode())
    return resp, (time.time() - t0) * 1000.0


def flatten(answers):
    tags, conf = {}, {}
    for k, v in (answers or {}).items():
        if not isinstance(v, dict):
            continue
        if "choice" in v:
            tags[k] = v.get("choice")
        elif "noul" in v:
            tags[k] = v.get("noul")
        elif "score" in v:
            tags[k] = v.get("score")
        if "confidence" in v:
            conf[k] = v.get("confidence")
    return tags, conf
