from json.decoder import JSONDecodeError
from time import sleep
from typing import Any, Generator

import pywikibot as pwb
import requests


WDQS_ENDPOINT = 'https://query.wikidata.org/sparql'
WDQS_USER_AGENT = f'{requests.utils.default_user_agent()} (Wikidata bot by User:MisterSynergy; mailto:mister.synergy@yahoo.com)'

WDQS_SLEEP = 1  # sec
WDQS_SLEEP_AFTER_TIMEOUT = 30  # sec
WDQS_SLICE_LIMIT = 100_000

WD = 'http://www.wikidata.org/entity/'
P = 'http://www.wikidata.org/prop/'

SITE = pwb.Site('wikidata', 'wikidata')
REPO = SITE.data_repository()


def query_wdqs(query:str, retry_counter:int=3) -> list[dict[str, dict[str, Any]]]:
    response = requests.post(
        url=WDQS_ENDPOINT,
        data={
            'query' : query,
        },
        headers={
            'User-Agent': WDQS_USER_AGENT,
            'Accept' : 'application/sparql-results+json',
        }
    )

    try:
        payload = response.json()
    except JSONDecodeError as exception:
        # nothing more left to slice on WDQS
        if response.elapsed.total_seconds() < 1 and 'offset is out of range' in response.text:
            return []

        # likely timed out, try again up to three times
        retry_counter -= 1
        if retry_counter > 0 and response.elapsed.total_seconds() > 55 and 'java.util.concurrent.TimeoutException' in response.text:
            sleep(WDQS_SLEEP_AFTER_TIMEOUT)
            return query_wdqs(query, retry_counter)

        raise RuntimeError(f'Cannot parse WDQS response as JSON; http status {response.status_code}; query time {response.elapsed.total_seconds():.2f} sec') from exception

    sleep(WDQS_SLEEP)

    return payload.get('results', {}).get('bindings', [])


def query_wdqs_sliced(query_template:str) -> Generator[dict[str, Any], None, None]:
    offset = 0
    while True:
        chunk = query_wdqs(
            query_template.format(
                offset=offset,
                limit=WDQS_SLICE_LIMIT,
            )
        )

        if len(chunk)==0:
            break

        for row in chunk:
            yield row

        offset += WDQS_SLICE_LIMIT


def adjust_ranks(qid:str, pid:str) -> None:
    if not qid.startswith('Q'):  # ignore property pages and lexeme pages for now
        return

    item = pwb.ItemPage(REPO, qid)

    if not item.exists():
        return
    if item.isRedirectPage():
        return

    item.get()

    for claim in item.claims.get(pid, []):
        if claim.rank == 'normal':
            return

    commands:dict[str, list] = { 'claims' : [] }
    for claim in item.claims.get(pid, []):
        claim_json = claim.toJSON()
        if claim_json.get('rank') != 'preferred':
            continue

        claim_json['rank'] = 'normal'
        commands['claims'].append(claim_json)

    if len(commands.get('claims', []))==0:
        return

    item.editEntity(commands, summary='remove unnecessary use of preferred rank #msynbotTask13')


def main() -> None:
    query_template = """SELECT DISTINCT ?item ?prop WHERE {{
        SERVICE bd:slice {{
            ?statement_node wikibase:rank wikibase:PreferredRank .
            bd:serviceParam bd:slice.offset {offset} .
            bd:serviceParam bd:slice.limit {limit} .
        }}
        ?item ?prop ?statement_node .
        FILTER NOT EXISTS {{ ?item ?prop [ wikibase:rank wikibase:NormalRank ] }}
    }}"""

    for row in query_wdqs_sliced(query_template):
        qid = row.get('item', {}).get('value', '').replace(WD, '')
        prop = row.get('prop', {}).get('value', '').replace(P, '')

        if len(qid)==0 or len(prop)==0:
            continue

        adjust_ranks(qid, prop)


if __name__=='__main__':
    main()
