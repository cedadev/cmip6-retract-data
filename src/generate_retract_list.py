
import os
import sys
import query_solr
import requests
import datetime
import math

retracted_file = "../withdrawls/retracted_datasets.txt"
today = str(datetime.date.today())
to_retract = "../withdrawls/to_retract_{}.txt".format(today)

def main():

    all_retracted_set = set()
    retracts_set= set()
    to_retract_set = set()
    with open(retracted_file) as r:
        [all_retracted_set.add(line.strip()) for line in r ]

    url = "https://esgf-index1.ceda.ac.uk/esg-search/search?type=Dataset&mip_era=CMIP6&replica=false&data_node!=esgf-data3.ceda.ac.uk&retracted=true&fields=id&format=application%2Fsolr%2Bjson&limit=10000"
    resp = requests.get(url)
    content = resp.json()
    total_retracts = content["response"]["numFound"]

    if total_retracts > 10000:
        iterations = math.ceil(total_retracts/10000.)

    for i in range(iterations):
        it = i * 10000
        url = "https://esgf-index1.ceda.ac.uk/esg-search/search?type=Dataset&mip_era=CMIP6&replica=false&data_node!=esgf-data3.ceda.ac.uk&retracted=true&fields=id&format=application%2Fsolr%2Bjson&offset={}&limit=10000".format(it)
        resp = requests.get(url)
        content = resp.json()

        for ds in content["response"]["docs"]:
            id = ds["id"].split("|")[0]
            retracts_set.add(id)

    to_retract_set = all_retracted_set - retracts_set
    
    print(len(all_retracted_set))
    print(len(to_retract_set))

    with open(retracted_file, "a+") as w:
        for id in list(to_retract_set):
            w.writelines("{}\n".format(id))

    with open(to_retract, "a+") as w:
        for id in list(to_retract_set):
            w.writelines("{}\n".format(id))


if __name__ == "__main__":

    # datasets_file = sys.argv[1]
    # main(datasets_file)
    main()
