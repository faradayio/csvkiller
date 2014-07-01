mkdir output

for f in tmp/*/*.csv
do
  ATTR=$(echo $f | awk -F '[/.]' '{print $(NF-1)}' -)
  head -n 1 $f > output/${ATTR}.csv
done

for f in tmp/*/*.csv
do
  echo "combining $f"
  ATTR=$(echo $f | awk -F '[/.]' '{print $(NF-1)}' -)
  tail -n +2 $f >> output/${ATTR}.csv
  rm $f;
done

rm -r tmp