![Project Logo](./img/The_Observer_candidate_2.png)
# The Observer

The goal of the project is to provide a 'no strings attatched' overview of public mastodon servers
leveraging the strength of the BERTopic model and the extensible architecture ofthe project

-------------------

## Architecture

Miro board [here](https://miro.com/app/board/uXjVMrHQaa4=/?share_link_id=492488903107)

![Architecture Diagram](./img/The_Observer_Architecture.png)

-------------------

## Setup

* It is preferable to run the project in a Linux environment or use wsl
* It is advised to run this project wither on the cloud, with a powerful VM instance or with at least 16GB or RAM and Linux

1) clone the directory with the git cli tool
2) chdir into the cloned directory
3) bash project_setup.sh
4) docker-compose build (This might take a while)

-------------------

## Run

4) docker-compose up -d (If you have limited hardware, it might take a while)

-------------------

## Configure Dashboard

5) in your local browser, browse http://localhost:5601/
6) Go to > Saved Objects > Import > TAP_Project/Data_Visualization/saved-objects/export_pre_final.ndjson > click import
7) Reload the page as is
8) Go to dashboard and select the 'the_observer' dashboard

# Notes

Do with the project as you please, I designed it to be expandable and hihgly maintainable, therefore you can add other servers, other social medias
so longs as you maintain the .jsonl format in the datastorage volume