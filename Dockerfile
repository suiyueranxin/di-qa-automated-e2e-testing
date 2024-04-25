# We need a Debian based image that has Python and unzip 
FROM docker.wdf.sap.corp:50000/python

RUN env

RUN python3 -m pip install --upgrade pip

# Install SAP NW RFC SDK and pynwrfc
# This is based on information from:
# - https://github.com/SAP/PyRFC/issues/176#issuecomment-630215733
# - https://github.com/SAP/fundamental-tools/tree/main/docker
ARG nwrfc_zip_file=nwrfcsdk.linuxx86_64-7.50.9.zip
ARG nwrfc_local=/usr/local/sap
RUN mkdir -p ${nwrfc_local}
ADD https://nexus.wdf.sap.corp:8443/nexus/content/groups/build.milestones/com/sap/conn/nwrfc/nwrfcsdk.linuxx86_64/7.50.9/${nwrfc_zip_file} ${nwrfc_local}
ENV SAPNWRFC_HOME ${nwrfc_local}/nwrfcsdk
RUN unzip ${nwrfc_local}/${nwrfc_zip_file} -d ${nwrfc_local} \
    && printf "\n# nwrfc sdk \n" >> ~/.bashrc \ 
    && printf "export SAPNWRFC_HOME=${nwrfc_local}/nwrfcsdk \n" >> ~/.bashrc \
    && chmod -R a+rx ${nwrfc_local}/nwrfcsdk/lib* \
    && printf "# include nwrfcsdk\n${nwrfc_local}/nwrfcsdk/lib\n" | tee /etc/ld.so.conf.d/nwrfcsdk.conf \
    && ldconfig \
    && pip install CYTHON \
    && PYRFC_BUILD_CYTHON=yes pip install pyrfc --no-binary :all:

# Install remaining dependencies
RUN pip install requests 
RUN pip install hdbcli
RUN pip install unittest-xml-reporting
RUN pip install coverage
RUN pip install azure-storage-file-datalake
RUN pip install pyarrow

# Unfortunately ENTRYPOINT and CMD don't work properly with Jenkins because
# Jenkins always adds a 'cat' command to the 'docker run'. This means that we
# we need to explicitly make the call to the python testrunner from outside
# which creates redundancy :-(

# ENTRYPOINT ["python3", "-m", "xmlrunner", "discover", "-s", "tests", "-o", "testresults"]
# CMD ["-p", "test*.py"]