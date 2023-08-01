#!/bin/bash

# usage: ./test-tempest-tests.sh {KEYSTONE_BRANCH} {TEST DIR}
set -x

# -e stops the script if any failures happen, -x is for debug output
# set -ex

KEYSTONE_BRANCH=${1:-stable/2023.1}
TEST_DIR=${2:-$(pwd)}
KEYSTONE_DIR=$TEST_DIR/keystone
TOX_DIR=$TEST_DIR/tox-venv


echo "### STEP 1: Deploy tox ###"
mkdir $TOX_DIR
# TODO: add tox version as an argument
TOX_VERSION='3.15.0'
python -m venv $TOX_DIR
source $TOX_DIR/bin/activate && pip install 'tox==3.15.0'

echo "### STEP 2: Download Keystone ###"

git clone -b $KEYSTONE_BRANCH https://github.com/openstack/keystone.git $KEYSTONE_DIR
cd $KEYSTONE_DIR && sed -i 's/pysaml2<4.0.3,>=2.4.0/pysaml2>=4.5.0/' requirements.txt

# TODO: put logic in to checkout a specific SHA1
# 1. read SHA1 as an arguement of script
# 2. run git reset --hard SHA1 inside of KEYSTONE_DIR
# 3. Possible change in requirements.txt of pysaml version

echo "### STEP 3: Install Packages ###"

# patch bindep step not included
# source $TOX_DIR/bin/activate && python # this command is not needed
source $TOX_DIR/bin/activate && pip install bindep
source $TOX_DIR/bin/activate && bindep --brief --file $KEYSTONE_DIR/bindep.txt

sudo systemctl start mariadb
sudo mysql --execute="CREATE USER 'keystone'@'localhost' IDENTIFIED BY 'SECRET';"
sudo mysql --execute="CREATE DATABASE keystone;"
sudo mysql --execute="GRANT ALL PRIVILEGES ON keystone.* TO 'keystone'@'localhost';"
sudo mysql --execute="FLUSH PRIVILEGES;"

echo "### STEP 4: Setup Venv ###"
cd $KEYSTONE_DIR && sed -i 's/usedevelop.*/usedevelop=false/g' tox.ini
cd $KEYSTONE_DIR && source $TOX_DIR/bin/activate && tox -e venv --notest
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && pip install 'python-openstackclient==5.2.1' 'osc-lib==2.0.0'

echo "### STEP 5: Configure Instance ###"
KEYREPO_DIR=$KEYSTONE_DIR/etc/fernet-keys

HOSTNAME=$(hostname -s)
ARCHIVE_DIR=$TEST_DIR/archive
mkdir -p $ARCHIVE_DIR
LOG_FILE=$ARCHIVE_DIR/keystone.$HOSTNAME.log
cd $KEYSTONE_DIR && source $TOX_DIR/bin/activate && tox -e genconfig
cd $KEYSTONE_DIR && cp -f $KEYSTONE_DIR/etc/keystone.conf.sample $KEYSTONE_DIR/etc/keystone.conf
cd $KEYSTONE_DIR && sed -e "s^#key_repository =.*^key_repository = $KEYREPO_DIR^" -i $KEYSTONE_DIR/etc/keystone.conf
cd $KEYSTONE_DIR && sed -e 's^#connection =.*^connection = mysql+pymysql://keystone:SECRET@localhost/keystone^' -i $KEYSTONE_DIR/etc/keystone.conf
cd $KEYSTONE_DIR && sed -e "s^#log_file =.*^log_file = $LOG_FILE^" -i $KEYSTONE_DIR/etc/keystone.conf
# cd $KEYSTONE_DIR && cp $KEYSTONE_DIR/etc/keystone.conf $ARCHIVE_DIR/keystone.$HOSTNAME.conf
cd $KEYSTONE_DIR && mkdir -p $KEYREPO_DIR
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && keystone-manage --config-file $KEYSTONE_DIR/etc/keystone.conf fernet_setup
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && keystone-manage  --config-file $KEYSTONE_DIR/etc/keystone.conf db_sync

echo "### STEP 6: Run Keystone ###"
# start the public endpoint
PUBLIC_PORT=5000
PUBLIC_HOST=localhost
OS_KEYSTONE_CONFIG_FILES=$KEYSTONE_DIR/etc/keystone.conf $KEYSTONE_DIR/.tox/venv/bin/python $KEYSTONE_DIR/.tox/venv/bin/keystone-wsgi-public --host $PUBLIC_HOST --port $PUBLIC_PORT &
KEYSTONE_PUBLIC_PID=$(pgrep -f keystone-wsgi-public)

# start the public endpoint
ADMIN_PORT=35357
ADMIN_HOST=localhost

# sleep driven synchronization
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && sleep 15

echo "### STEP 7: Fill Keystone ###"
PUBLIC_URL=http://$PUBLIC_HOST:$PUBLIC_PORT/v3

cd $KEYSTONE_DIR && source .tox/venv/bin/activate && keystone-manage --config-file $KEYSTONE_DIR/etc/keystone.conf bootstrap --bootstrap-password ADMIN --bootstrap-region-id RegionOne --bootstrap-internal-url $PUBLIC_URL --bootstrap-admin-url $PUBLIC_URL --bootstrap-public-url $PUBLIC_URL

cd $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack service create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $PUBLIC_URL --description 'Swift Service' --name swift object-store --debug

cd $KEYSTONE_DIR && source .tox/venv/bin/activate && sleep 3

RGW_ENDPOINT=http://localhost:8000
cd $KEYSTONE_DIR && source .tox/venv/bin/activate && openstack endpoint create --os-username admin --os-password ADMIN --os-user-domain-id default --os-project-name admin --os-project-domain-id default --os-identity-api-version 3 --os-auth-url $PUBLIC_URL swift public "$RGW_ENDPOINT/v1/KEY_\$(tenant_id)s" --debug

# radosgw needs to be started up after this point
cd $TEST_DIR
MON=1 OSD=1 RGW=1 MGR=0 MDS=0 ../src/vstart.sh -n -d
ps ax | grep ceph

echo "### STEP 8: Download Tempest ###"

TEMPEST_BRANCH=${3:-master}

TEMPEST_DIR=$TEST_DIR/tempest
git clone -b $TEMPEST_BRANCH https://github.com/openstack/tempest.git $TEMPEST_DIR
cd $TEMPEST_DIR && git reset --hard 34.1.0

echo "### STEP 9: Setup Venv for Tempest ###"
cd $TEMPEST_DIR && source $TOX_DIR/bin/activate && tox -e venv --notest
cd $TEMPEST_DIR && source .tox/venv/bin/activate && tempest init --workspace-path $TEMPEST_DIR/workspace.yaml rgw

echo "### STEP 10: Configure Instance for Tempest ###"
cp $TEST_DIR/tempest.conf $TEMPEST_DIR/rgw/etc/tempest.conf

echo "### STEP 11: Run Tempest ###"
cd $TEMPEST_DIR && source .tox/venv/bin/activate && tempest run --workspace-path $TEMPEST_DIR/workspace.yaml --workspace rgw --regex '^tempest.api.object_storage' --black-regex '.*test_account_quotas_negative.AccountQuotasNegativeTest.test_user_modify_quota|.*test_container_acl_negative.ObjectACLsNegativeTest.*|.*test_container_services_negative.ContainerNegativeTest.test_create_container_metadata_.*|.*test_container_staticweb.StaticWebTest.test_web_index|.*test_container_staticweb.StaticWebTest.test_web_listing_css|.*test_container_synchronization.*|.*test_object_services.PublicObjectTest.test_access_public_container_object_without_using_creds|.*test_object_services.ObjectTest.test_create_object_with_transfer_encoding|.*test_object_expiry.ObjectExpiryTest.test_get_object_after_expiry_time|.*test_object_expiry.ObjectExpiryTest.test_get_object_at_expiry_time|.*test_account_services.AccountTest.test_list_no_account_metadata'

echo "### CLEAN UP BEGINNING ###"

KEYSTONE_PUBLIC_PID=$(pgrep -f keystone-wsgi-public)
cd $TEST_DIR
rm -rf tempest
../src/stop.sh
## Stoping Keyston Public Instance
kill $KEYSTONE_PUBLIC_PID

sudo systemctl stop mariadb
rm -rf archive
rm -rf keystone
rm -rf tox-venv

# To run a single test
# cd tempest && source .tox/venv/bin/activate
# tempest init --workspace-path ~/ssd/ceph/master/build/tempest/workspace.yaml rgw
# cp ../tempest.conf rgw/etc/
# tempest run --workspace-path ~/ssd/ceph/master/build/tempest/workspace.yaml --workspace rgw --regex '^tempest.api.object_storage.test_container_quotas.ContainerQuotasTest.test_upload_too_many_objects'

echo "### CLEAN UP: Keystone Dir Removed ###"
