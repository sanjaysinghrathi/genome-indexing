#set -xv

USERNAME=`whoami`
FILE=$1
BASENAME=`basename $1`
SIZE=$(wc -c $FILE | cut -d' ' -f1)
SIZE=$(expr $SIZE - 1)
MAPPERS=$2
REDUCERS=$3
if [ $SIZE -lt $MAPPERS ]; then
  MAPPERS=$SIZE
fi
INTERVAL=$((SIZE/MAPPERS+1))
HDFS_ADDR="mshadoop1"
./build.sh
HPATH=/user/$USERNAME/psort
hadoop fs -rmr $HPATH/bsout
hadoop fs -rm "$HPATH/dc3_data/seq.txt"
i=0
rm -rf seq.txt
for (( i=0; i+INTERVAL-1 < SIZE; i+=INTERVAL ))
  do
    k=$((i+INTERVAL))
    echo $i,$k >> seq.txt
done
echo $i,$((SIZE)) >> seq.txt

hadoop fs -copyFromLocal seq.txt "$HPATH/dc3_data/seq.txt"
hadoop fs -rm "$HPATH/dc3_data/dna_filename"
hadoop fs -copyFromLocal $FILE "$HPATH/dc3_data/dna_filename"

# Create the partition file.
hadoop fs -rm "$HPATH/dc3_data/partition"
./sampler $FILE $REDUCERS > partition
hadoop fs -copyFromLocal partition "$HPATH/dc3_data/partition"

time hadoop jar SimpleBucketSorter.jar bucket_sort.SimpleBucketSorter "hdfs://${HDFS_ADDR}${HPATH}/dc3_data/seq.txt" $HPATH/bsout $REDUCERS "hdfs://${HDFS_ADDR}${HPATH}/dc3_data/dna_filename#dna_filename" "hdfs://${HDFS_ADDR}${HPATH}/libraries/libpsort.so#libpsort.so" "$HPATH/dc3_data/partition" $BASENAME
