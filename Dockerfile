#
# Copyright 2017 Apereo Foundation (AF) Licensed under the
# Educational Community License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may
# obtain a copy of the License at
#
#     http://opensource.org/licenses/ECL-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS IS"
# BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing
# permissions and limitations under the License.
#

#
# Setup in two steps
#
# Step 1: Build the image
# $ docker build -f Dockerfile -t hilary:latest .
# Step 2: Run the docker
# $ docker run -it --name=node --net=host -v /src/brecke/Hilary:/usr/src/Hilary -v /src/brecke/3akai-ux:/usr/src/3akai-ux hilary:latest
#

# FROM buildpack-deps:jessie-scm
FROM oaeproject/oae-hilary-deps
LABEL Name=hilary Version=12.5.0
MAINTAINER Apereo Foundation <which.email@here.question>

# Install global dependencies
RUN npm install --global nodemon
RUN npm install --global bunyan
RUN npm install --global grunt

# Create the temp directory
RUN mkdir -p /tmp/oae

# Set the base directory
RUN mkdir -p /usr/src/Hilary
WORKDIR /usr/src/Hilary

# Install 3akai-ux dependencies
RUN mkdir -p /usr/src/Hilary/3akai-ux/node_modules
COPY ./3akai-ux/node_modules /usr/src/Hilary/3akai-ux/node_modules/

COPY ./3akai-ux/package.json /usr/src/Hilary/3akai-ux/
RUN cd /usr/src/Hilary/3akai-ux && npm install

# Install hilary dependencies
RUN mkdir -p /usr/src/Hilary/node_modules
COPY ./node_modules/ /usr/src/Hilary/node_modules/

COPY package.json /usr/src/Hilary/
RUN cd /usr/src/Hilary && npm install

# Expose ports for node server
EXPOSE 2000
EXPOSE 2001

# Run the app - you may override CMD via docker run command line instruction
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["nodemon -L app.js | bunyan"]
