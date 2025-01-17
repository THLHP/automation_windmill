src="${1:-/path/first}"
dst="${2:-/path/second/}"

PRIVATE_KEY=$(curl -s -H "Authorization: Bearer $WM_TOKEN" \
  "$BASE_INTERNAL_URL/api/w/$WM_WORKSPACE/variables/get_value/f/dicoms/asu_vm_private_key" | jq -r .)

KNOWN_HOSTS=$(curl -s -H "Authorization: Bearer $WM_TOKEN" \
  "$BASE_INTERNAL_URL/api/w/$WM_WORKSPACE/variables/get_value/f/dicoms/vultra-sa-identy" | jq -r .)

set -e  # Exit on any error

# Prevent apt from prompting for input
export DEBIAN_FRONTEND=noninteractive

# Update package lists and install rsync
apt-get update && apt-get install -y rsync

# Verify installation
which rsync || { echo "rsync installation failed"; exit 1; }

echo "Source $src"
echo "Destination $dst"

khostfile=$(mktemp)
echo "$KNOWN_HOSTS" > "$khostfile"

tmpfile=$(mktemp)
echo "$PRIVATE_KEY" > "$tmpfile"

chmod 600 "$tmpfile" # Ensure proper permissions
rsync -avvz -e "ssh -i $tmpfile -o UserKnownHostsFile=$khostfile" $src $dst
rm "$tmpfile" # Clean up the temporary file
rm "$khostfile"
