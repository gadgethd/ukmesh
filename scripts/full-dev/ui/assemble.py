#!/usr/bin/env python3
"""Package DEV UI assets after each frontend's own build checks."""

import argparse
from pathlib import Path
import shutil

from control import ARTIFACTS, HERE, REPO, SERVICES, release_path

BASELINE = ARTIFACTS.parent / 'baseline'
PUBLIC_ASSETS = REPO / 'frontend/public'
THEME = PUBLIC_ASSETS / 'ukmesh-theme'
BASELINE_DIRECTORIES = {'app': 'html', 'website': 'html', 'website-dev': 'html',
                        'health': 'public', 'beacon-web': 'html', 'corescope': 'public', 'hub': 'site'}


def seed_classic(name):
    if not name.startswith('classic') or len(name) > 48 or not name.replace('-', '').isalnum():
        raise ValueError('invalid classic release ID')
    classic = ARTIFACTS / name
    classic.mkdir(exist_ok=True)
    if (classic / 'receipt.json').exists():
        return classic
    for _, _, container, key, _, _ in SERVICES:
        shutil.copytree(BASELINE / container / BASELINE_DIRECTORIES[key], classic / key, dirs_exist_ok=True)
    return classic


def theme_corescope(path, url, *, embed_live=False):
    theme = path / 'ukmesh-theme'
    shutil.copytree(THEME, theme, dirs_exist_ok=True)
    shutil.copy2(HERE / 'corescope.css', theme / 'corescope.css')
    script = '/ukmesh-theme/corescope-live-embed.js'
    if embed_live:
        shutil.copy2(HERE / 'corescope-live-embed.js', theme / Path(script).name)
    for page in path.glob('*.html'):
        html = page.read_text()
        additions = []
        if '/ukmesh-theme/corescope.css' not in html:
            additions.append(f'  <link rel="stylesheet" href="{url}">\n')
        if embed_live and script not in html:
            additions.append(f'  <script defer src="{script}"></script>\n')
        if additions and '</head>' in html:
            page.write_text(html.replace('</head>', ''.join(additions) + '</head>', 1))


def retain_assets(source, target):
    if not source.is_dir():
        return
    target.mkdir(exist_ok=True, parents=True)
    for asset in source.iterdir():
        if asset.is_file() and not (target / asset.name).exists():
            shutil.copy2(asset, target)


def assemble(name, staged_core, classic_name):
    release = release_path(name)
    if name == 'classic' or (release / 'receipt.json').exists():
        raise ValueError('assembly requires an unsealed candidate')
    classic = seed_classic(classic_name)
    health = release / 'health'
    shutil.copytree(classic / 'health', health, dirs_exist_ok=True)
    shutil.copytree(REPO / 'third_party/meshcore-health-check/public', health, dirs_exist_ok=True)
    shutil.copytree(THEME, health / 'ukmesh-theme', dirs_exist_ok=True)
    shutil.copytree(REPO / 'scripts/full-dev/lan/site', release / 'hub', dirs_exist_ok=True)
    shutil.copytree(THEME, release / 'beacon-web/ukmesh-theme', dirs_exist_ok=True)
    shutil.copytree(PUBLIC_ASSETS / 'fonts', release / 'beacon-web/fonts', dirs_exist_ok=True)
    shutil.copytree(classic / 'corescope', release / 'corescope', dirs_exist_ok=True)
    theme_corescope(release / 'corescope', '/ukmesh-theme/corescope.css', embed_live=True)
    if staged_core:
        shutil.copytree(staged_core, release / 'corescope-staged', dirs_exist_ok=True)
        theme_corescope(release / 'corescope-staged', '/corescope/static/ukmesh-theme/corescope.css')
    for key in ('app', 'website', 'website-dev', 'beacon-web'):
        old, new = classic / key / 'assets', release / key / 'assets'
        if not (classic / 'receipt.json').exists():
            retain_assets(new, old)
        retain_assets(old, new)


if __name__ == '__main__':
    cli = argparse.ArgumentParser()
    cli.add_argument('release')
    cli.add_argument('--staged-core', type=Path)
    cli.add_argument('--classic-name', default='classic')
    args = cli.parse_args()
    assemble(args.release, args.staged_core, args.classic_name)
