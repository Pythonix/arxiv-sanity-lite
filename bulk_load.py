import time
import logging
import urllib.request
from collections import OrderedDict
import xml.etree.ElementTree as ET
from aslite.db import get_papers_db, get_metas_db


def get_response(category, resumption_token=None):
    oai_url = f'http://export.arxiv.org/oai2?verb=ListRecords'
    if resumption_token is not None:
        oai_url += f'&resumptionToken={resumption_token}'
    else:
        oai_url += f"&set={category}&metadataPrefix=arXiv"

    with urllib.request.urlopen(oai_url) as url:
        response = url.read()

    if url.status != 200:
        logger.error(f"arxiv did not return status 200 response")

    return response

def parse(response):
    categories = ["cs.CV", "cs.LG", "cs.CL", "cs.AI", "cs.NE", "cs.RO", "cs.IT"]
    pdb = get_papers_db(flag="c")
    mdb = get_metas_db(flag="c")
    root = ET.fromstring(response)
    for t in root.iter("{http://www.openarchives.org/OAI/2.0/}record"):
        data = {}
        metadata = t.find("{http://www.openarchives.org/OAI/2.0/}metadata/{http://arxiv.org/OAI/arXiv/}arXiv")
        data["id"] = f"http://arxiv.org/abs/{metadata.find('{http://arxiv.org/OAI/arXiv/}id').text}"
        data["guidislink"] = True
        data["link"] = data["id"]
        data["published"] = metadata.find("{http://arxiv.org/OAI/arXiv/}created").text
        data["updated"] = metadata.find("{http://arxiv.org/OAI/arXiv/}updated")
        if data["updated"] is None:
            data["updated"] = data["published"]
        else:
            data["updated"] = data["updated"].text
        data["published_parsed"] = time.strptime(data["published"], "%Y-%m-%d")
        data["updated_parsed"] = time.strptime(data["updated"], "%Y-%m-%d")
        data["title"] = metadata.find("{http://arxiv.org/OAI/arXiv/}title").text
        data["summary"] = metadata.find("{http://arxiv.org/OAI/arXiv/}abstract").text
        data["authors"] = []

        for author in metadata.find("{http://arxiv.org/OAI/arXiv/}authors"):
            if (forename := author.find("{http://arxiv.org/OAI/arXiv/}forename")) is None:
                forename = author.find("{http://arxiv.org/OAI/arXiv/}forenames")
            if forename is not None:
                name = author.find("{http://arxiv.org/OAI/arXiv/}keyname").text + " " + forename.text
            else:
                name = author.find("{http://arxiv.org/OAI/arXiv/}keyname").text
            data["authors"].append({
                "name": name
            })

        data["author_detail"] = data["authors"][-1]
        data["author"] = data["authors"][-1]
        data["links"] = [{
            "href": data["id"],
            "rel": "alternate",
            "type": "text/html"
        },{
            "title": "pdf",
            "href": data["id"].replace("abs", "pdf"),
            "rel": "related",
            "type": "application/pdf"
        }]
        data["arxiv_primary_category"] = {
            "term": metadata.find("{http://arxiv.org/OAI/arXiv/}categories").text.split(" ")[0],
            "scheme": "http://arxiv.org/schemas/atom"
        }
        data["tags"] = [
            {
                "term": cat,
                "scheme": "http://arxiv.org/schemas/atom",
                "label": None
            }
            for cat in metadata.find("{http://arxiv.org/OAI/arXiv/}categories").text.split(" ") 
        ]
        data["_idv"] = metadata.find("{http://arxiv.org/OAI/arXiv/}id").text
        data["_id"] = metadata.find("{http://arxiv.org/OAI/arXiv/}id").text
        data["_version"] = 1
        data["_time"] = time.mktime(data["updated_parsed"])
        data["_time_str"] = time.strftime("%b %d %Y", data["updated_parsed"])

        correct_category = False
        for cat in data["tags"]:
            if cat["term"] in categories:
                correct_category = True
                break
        if correct_category:
            pid = data["_id"]
            if pid in pdb:
                if data['_time'] > pdb[pid]['_time']:
                    pdb[data['_id']] = data
                    mdb[data['_id']] = {'_time': data['_time']}
            else:
                pdb[data['_id']] = data
                mdb[data['_id']] = {'_time': data['_time']}
            
    return root.find("{http://www.openarchives.org/OAI/2.0/}ListRecords/{http://www.openarchives.org/OAI/2.0/}resumptionToken").text


    
if __name__ == "__main__":
    response = get_response("cs")
    while True:
        resumption_token = parse(response)
        print("Resumption Token", resumption_token)
        if resumption_token is None:
            break
        time.sleep(5)
        response = get_response("cs", resumption_token)
