"""Render editable Mermaid sources as portable SVG previews and update the index.

Requires Node.js/npx and a Chrome installation (set CHROME_BIN if needed).
Optional --no-sandbox is for trusted rendering in isolated containers only.
"""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT=Path(__file__).resolve().parent
VERSION='12.2.0'
NAMES=[('document-analysis-and-research','Document analysis and durable local research'),('simple-search-flow','Search and streaming'),('independent-web-ingestion','Independent webpage and asset processing'),('asset-lifecycle','Asset lifecycle'),('collection-publication','Collection publication'),('provenance-flow','Provenance'),('source-rechecks','Source rechecks'),('visual-caption-location-flow','Images, captions and approved locations')]

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--no-sandbox',action='store_true')
    parser.add_argument('--only',choices=[name for name,_ in NAMES])
    args=parser.parse_args()
    if not shutil.which('npx'):raise SystemExit('Install Node.js/npm to render diagrams')
    browser=os.environ.get('CHROME_BIN') or next((p for name in ('google-chrome','chromium','chromium-browser') if (p:=shutil.which(name))),None)
    if not browser:raise SystemExit('Set CHROME_BIN to your Chrome executable')
    config={'executablePath':browser,'args':['--no-sandbox'] if args.no_sandbox else []}
    with tempfile.TemporaryDirectory(prefix='diagram-render-') as tmp:
        browser_config=Path(tmp)/'browser.json';browser_config.write_text(json.dumps(config))
        for name,_ in NAMES:
            if args.only and name!=args.only:continue
            subprocess.run(['npx','--yes','--package','@mermaid-js/mermaid-cli@'+VERSION,'mmdc','-i',str(ROOT/(name+'.mmd')),'-o',str(ROOT/(name+'.svg')),'-c',str(ROOT/'mermaid-config.json'),'-p',str(browser_config),'-b','white'],check=True,env={**os.environ,'PUPPETEER_SKIP_DOWNLOAD':'true'})
    text='# Functional flows\n\nClick a diagram to open its full-size SVG. Labels use native SVG text; editable Mermaid sources are linked below each preview.\n'
    for name,title in NAMES:
        text+='\n## '+title+'\n\n[!['+title+']('+name+'.svg)]('+name+'.svg)\n\n[Mermaid source]('+name+'.mmd) · [Full-size SVG]('+name+'.svg)\n'
    text+='\n## Regenerate previews\n\nInstall Node.js/npm and Chrome, then run `python3 diagrams/render.py`. Set `CHROME_BIN` if Chrome is not on PATH. Previews are checked in so viewing the diagrams does not require a local renderer.\n'
    (ROOT/'README.md').write_text(text)

if __name__=='__main__':main()
