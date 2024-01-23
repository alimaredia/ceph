set -ex

# TODO: dbstore_dir

BUILD_DIR=$1
TEST_DIR=$(pwd)

cd $BUILD_DIR

../src/stop.sh
ninja vstart
echo "TEST #1: ZONE CREATE - RADOS BACKEND - WITH SAL CONFIG"
MON=1 OSD=1 MDS=0 RGW=1 MGR=0 ../src/vstart.sh -n -d
./bin/radosgw-admin -c ceph.conf zone create --rgw-zone=zone1 --sal-config-file=$TEST_DIR/infile-rados.json
./bin/radosgw-admin -c ceph.conf zone get --rgw-zone=zone1 | jq -rM '.sal_config' > testing.out
diff testing.out $TEST_DIR/infile-rados.json
echo "TEST #1: PASSED"
echo "TEST #2: ZONE CREATE - DBSTORE BACKEND - WITH SAL CONFIG"
../src/stop.sh
rm -rf ~/dbstore_dir/*
#MON=0 OSD=0 MDS=0 RGW=0 MGR=0 ../src/vstart.sh -n -d
./bin/radosgw-admin -c dbstore_ceph.conf zone create --rgw-zone=zone2 --sal-config-file=$TEST_DIR/infile-dbstore.json
./bin/radosgw-admin -c dbstore_ceph.conf zone get --rgw-zone=zone2 | jq -rM '.sal_config' > testing.out
diff testing.out $TEST_DIR/infile-dbstore.json
rm -rf ~/dbstore_dir/*
echo "TEST #2: PASSED"
echo "TEST #3: ZONE CREATE - RADOS BACKEND - WITHOUT SAL CONFIG"
MON=1 OSD=1 MDS=0 RGW=1 MGR=0 ../src/vstart.sh -n -d
./bin/radosgw-admin -c ceph.conf zone create --rgw-zone=zone3
./bin/radosgw-admin -c ceph.conf zone get --rgw-zone=zone3 | jq -rM '.sal_config' > testing.out
diff testing.out $TEST_DIR/infile-rados.json
../src/stop.sh
echo "TEST #3: PASSED"
echo "TEST #4: ZONE CREATE - DBSTORE BACKEND - WITHOUT SAL CONFIG"
rm -rf ~/dbstore_dir/*
./bin/radosgw-admin -c dbstore_ceph.conf zone create --rgw-zone=zone4
./bin/radosgw-admin -c dbstore_ceph.conf zone get --rgw-zone=zone4 | jq -rM '.sal_config' > testing.out
diff testing.out $TEST_DIR/infile-dbstore-default.json
rm -rf ~/dbstore_dir/*
echo "TEST #4: PASSED"
echo "TEST #5: RADOSGW - RADOS BACKEND - WITH CUSTOM SAL CONFIG IN ZONE"
rm -rf out
MON=1 OSD=1 MDS=0 RGW=0 MGR=0 ../src/vstart.sh -n -d
cat ceph.conf $TEST_DIR/rgw_ceph.conf > new_ceph.conf
./bin/radosgw-admin -c ceph.conf zone create --rgw-zone=zone5 --sal-config-file=$TEST_DIR/infile-rados.json
./bin/ceph auth get-or-create client.rgw.8000 mon 'allow rw' osd 'allow rwx' mgr 'allow rw' >> keyring
./bin/radosgw -c /home/fedora/ceph/build/new_ceph.conf --log-file=/home/fedora/ceph/build/out/radosgw.8000.log --admin-socket=/home/fedora/ceph/build/out/radosgw.8000.asok --pid-file=/home/fedora/ceph/build/out/radosgw.8000.pid --rgw_luarocks_location=/home/fedora/ceph/build/out/radosgw.8000.luarocks --debug-rgw=20 --debug-ms=1 -n client.rgw.8000 --rgw_frontends='beast port=8000' --rgw_zone=zone5
sleep 3
grep -c "Radosgw is in zone: zone5" out/radosgw.8000.log > testing.out
diff testing.out $TEST_DIR/one.txt
../src/stop.sh
echo "TEST #5: PASSED"
echo "TEST #6: RADOSGW - DBSTORE BACKEND - WITH CUSTOM SAL CONFIG IN ZONE"
rm -rf out
rm -rf ~/dbstore_dir/*
./bin/radosgw-admin -c dbstore_ceph.conf zone create --rgw-zone=zone6 --sal-config-file=$TEST_DIR/infile-dbstore.json
./bin/radosgw -c /home/fedora/ceph/build/dbstore_ceph.conf --log-file=/home/fedora/ceph/build/out/radosgw.8000.log --admin-socket=/home/fedora/ceph/build/out/radosgw.8000.asok --pid-file=/home/fedora/ceph/build/out/radosgw.8000.pid --rgw_luarocks_location=/home/fedora/ceph/build/out/radosgw.8000.luarocks --debug-rgw=20 --debug-ms=1 -n client.rgw.8000 --rgw_frontends='beast port=8000' --rgw_zone=zone6
sleep 3
grep -c "Radosgw is in zone: zone6" out/radosgw.8000.log > testing.out
diff testing.out $TEST_DIR/one.txt
rm -rf ~/dbstore_dir/*
echo "TEST #6: PASSED"
echo "TEST #7: RADOSGW - RADOS BACKEND - WITH DEFAULT SAL CONFIG IN ZONE"
rm -rf out
MON=1 OSD=1 MDS=0 RGW=0 MGR=0 ../src/vstart.sh -n -d
./bin/radosgw-admin -c ceph.conf user create --uid test4 --display-name "test4" --access-key=test4 --secret-key=test4
cat ceph.conf $TEST_DIR/rgw_ceph.conf > new_ceph.conf
./bin/ceph auth get-or-create client.rgw.8000 mon 'allow rw' osd 'allow rwx' mgr 'allow rw' >> keyring
./bin/radosgw -c /home/fedora/ceph/build/new_ceph.conf --log-file=/home/fedora/ceph/build/out/radosgw.8000.log --admin-socket=/home/fedora/ceph/build/out/radosgw.8000.asok --pid-file=/home/fedora/ceph/build/out/radosgw.8000.pid --rgw_luarocks_location=/home/fedora/ceph/build/out/radosgw.8000.luarocks --debug-rgw=20 --debug-ms=1 -n client.rgw.8000 --rgw_frontends='beast port=8000'
sleep 3
grep -c "Radosgw is in zone: default" out/radosgw.8000.log > testing.out
diff testing.out $TEST_DIR/one.txt
../src/stop.sh
echo "TEST #7: PASSED"
echo "TEST #8: RADOSGW - DBSTORE BACKEND - WITH DEFAULT SAL CONFIG IN ZONE"
rm -rf out
rm -rf ~/dbstore_dir/*
./bin/radosgw -c /home/fedora/ceph/build/dbstore_ceph.conf --log-file=/home/fedora/ceph/build/out/radosgw.8000.log --admin-socket=/home/fedora/ceph/build/out/radosgw.8000.asok --pid-file=/home/fedora/ceph/build/out/radosgw.8000.pid --rgw_luarocks_location=/home/fedora/ceph/build/out/radosgw.8000.luarocks --debug-rgw=20 --debug-ms=1 -n client.rgw.8000 --rgw_frontends='beast port=8000'
sleep 3
grep -c "Radosgw is in zone: default" out/radosgw.8000.log > testing.out
diff testing.out $TEST_DIR/one.txt
rm -rf ~/dbstore_dir/*
echo "TEST #8: PASSED"
../src/stop.sh
