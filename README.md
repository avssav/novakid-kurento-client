[![License badge](https://img.shields.io/badge/license-Apache2-orange.svg)](http://www.apache.org/licenses/LICENSE-2.0)
[![Documentation badge](https://readthedocs.org/projects/fiware-orion/badge/?version=latest)](http://doc-kurento.readthedocs.org/en/latest/)
[![Docker badge](https://img.shields.io/docker/pulls/fiware/orion.svg)](https://hub.docker.com/r/fiware/stream-oriented-kurento/)
[![Support badge]( https://img.shields.io/badge/support-sof-yellowgreen.svg)](http://stackoverflow.com/questions/tagged/kurento)

[![][KurentoImage]][Kurento]


Copyright © 2017-2020 [Novakid]. Licensed under [Apache 2.0 License].

novakid-kurento-client
======================

A. If you don't want to install novakid-kurento-client to your computer you can see it here
https://test.novakidschool.com:8899/

For tests from one your computer: You can open this URL in Firefox (Chrome). 
Open "New incognito window" and open URL again there.

You can't open from 2 different browsers(for ex. Chrome and Firefox) at the same time because 
one of them will occupy the video camera. And the second one will not permited to access it.


B. If you want install novakid-kurento-client to your computer you need:

1. Install nodejs and yarn

curl -sL https://deb.nodesource.com/setup_8.x | sudo -E bash -
sudo apt-get install -y nodejs gcc g++ make

curl -sL https://dl.yarnpkg.com/debian/pubkey.gpg | sudo apt-key add -
echo "deb https://dl.yarnpkg.com/debian/ stable main" | sudo tee /etc/apt/sources.list.d/yarn.list
sudo apt-get update && sudo apt-get install yarn

2. Download client, make tmp directory and install client.

git clone https://github.com/avssav/novakid-kurento-client
mkdir -p /srv/novakid-records 
cd novakid-kurento-client
npm install

3. Make systemd init script (We use Ubuntu 16.04):

echo "
[Unit]
Description=One-to-one video call using WebRTC technology
Documentation=https://www.kurento.org/documentation
After=network.target

[Service]
Type=simple
User=novakid
Group=novakid
WorkingDirectory=/home/novakid/novakid-kurento-client
LimitNOFILE=131072
ExecStart=/usr/bin/npm start -- --ws_uri=ws://test.novakidschool.com:8888/kurento --records_path=file:///srv/novakid-records/ --stats_timeout=10 --ssl=1 --port=8899
Restart=always
RestartSec=10s

[Install]
WantedBy=multi-user.target
" | sudo tee /etc/systemd/system/novakid-kurento-client.service

4. Enable, run and check it.

sudo systemctl enable novakid-kurento-client.service
sudo systemctl start novakid-kurento-client.service
sudo systemctl status novakid-kurento-client.service

5. Open client on your computer (with SSL errors):

For example: https://192.168.2.3:8899/


Copyright © 2013-2016 [Kurento]. Licensed under [Apache 2.0 License].

kurento-one2one-call
====================

Kurento Java Tutorial: WebRTC one to one video call.

Running this tutorial
---------------------

In order to run this tutorial, please read the following [instructions].

What is Kurento
---------------

Kurento is an open source software project providing a platform suitable
for creating modular applications with advanced real-time communication
capabilities. For knowing more about Kurento, please visit the Kurento
project website: http://www.kurento.org.

Kurento is part of [FIWARE]. For further information on the relationship of
FIWARE and Kurento check the [Kurento FIWARE Catalog Entry]

Kurento is part of the [NUBOMEDIA] research initiative.

Documentation
-------------

The Kurento project provides detailed [documentation] including tutorials,
installation and development guides. A simplified version of the documentation
can be found on [readthedocs.org]. The [Open API specification] a.k.a. Kurento
Protocol is also available on [apiary.io].

Source
------

Code for other Kurento projects can be found in the [GitHub Kurento Group].

News and Website
----------------

Check the [Kurento blog]
Follow us on Twitter @[kurentoms].

Issue tracker
-------------

Issues and bug reports should be posted to the [GitHub Kurento bugtracker]

Licensing and distribution
--------------------------

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Contribution policy
-------------------

You can contribute to the Kurento community through bug-reports, bug-fixes, new
code or new documentation. For contributing to the Kurento community, drop a
post to the [Kurento Public Mailing List] providing full information about your
contribution and its value. In your contributions, you must comply with the
following guidelines

* You must specify the specific contents of your contribution either through a
  detailed bug description, through a pull-request or through a patch.
* You must specify the licensing restrictions of the code you contribute.
* For newly created code to be incorporated in the Kurento code-base, you must
  accept Kurento to own the code copyright, so that its open source nature is
  guaranteed.
* You must justify appropriately the need and value of your contribution. The
  Kurento project has no obligations in relation to accepting contributions
  from third parties.
* The Kurento project leaders have the right of asking for further
  explanations, tests or validations of any code contributed to the community
  before it being incorporated into the Kurento code-base. You must be ready to
  addressing all these kind of concerns before having your code approved.

Support
-------

The Kurento project provides community support through the  [Kurento Public
Mailing List] and through [StackOverflow] using the tags *kurento* and
*fiware-kurento*.

Before asking for support, please read first the [Kurento Netiquette Guidelines]

[documentation]: http://www.kurento.org/documentation
[FIWARE]: http://www.fiware.org
[GitHub Kurento bugtracker]: https://github.com/Kurento/bugtracker/issues
[GitHub Kurento Group]: https://github.com/kurento
[kurentoms]: http://twitter.com/kurentoms
[Kurento]: http://kurento.org
[Kurento Blog]: http://www.kurento.org/blog
[Kurento FIWARE Catalog Entry]: http://catalogue.fiware.org/enablers/stream-oriented-kurento
[Kurento Netiquette Guidelines]: http://www.kurento.org/blog/kurento-netiquette-guidelines
[Kurento Public Mailing list]: https://groups.google.com/forum/#!forum/kurento
[KurentoImage]: https://secure.gravatar.com/avatar/21a2a12c56b2a91c8918d5779f1778bf?s=120
[Apache 2.0 License]: http://www.apache.org/licenses/LICENSE-2.0
[NUBOMEDIA]: http://www.nubomedia.eu
[StackOverflow]: http://stackoverflow.com/search?q=kurento
[Read-the-docs]: http://read-the-docs.readthedocs.org/
[readthedocs.org]: http://kurento.readthedocs.org/
[Open API specification]: http://kurento.github.io/doc-kurento/
[apiary.io]: http://docs.streamoriented.apiary.io/
[instructions]: http://www.kurento.org/docs/current/tutorials/node/tutorial-one2one.html
