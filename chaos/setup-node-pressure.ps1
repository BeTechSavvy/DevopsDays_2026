# chaos/setup-node-pressure.ps1
#
# Lets node 2 (minikube-m02) report MemoryPressure and evict pods, which
# chaos/node_pressure.py needs. Run once after every `minikube start`
# (minikube regenerates the kubelet config on start):
#
#   powershell -ExecutionPolicy Bypass -File .\chaos\setup-node-pressure.ps1
#   powershell -ExecutionPolicy Bypass -File .\chaos\setup-node-pressure.ps1 -Undo
#
# Why: minikube's kubelet config sets evictionHard without memory.available,
# which disables memory eviction entirely. And under the Docker driver the
# kubelet sees the whole WSL VM's memory (~10 GB) while the node container
# is capped at 4 GB -- so without a threshold, a memory hog OOM-kills the
# node container itself instead of the kubelet evicting the pod.
# The kubelet's "memory.available" is its capacity (~9.7 GiB) minus ITS
# working set of the pods (~0.5 GiB idle on node 2) -- check it with
#   kubectl get --raw /api/v1/nodes/minikube-m02/proxy/stats/summary
# A threshold of 8Gi means pressure once the kubelet sees more than ~1.7 GiB
# in use: idle leaves ~1.2 GiB of margin, the 1.6 GiB stress pod crosses it,
# and the node container itself stays around 2.5 of its 4 GiB.
#
# Only node 2 is changed. A backup of the original is kept at config.yaml.bak.

param([switch]$Undo, [string]$Threshold = "8Gi")

$node = "minikube-m02"
$cfg = "/var/lib/kubelet/config.yaml"

if ($Undo) {
    minikube ssh -n $node -- "sudo cp $cfg.bak $cfg && sudo systemctl restart kubelet"
    Write-Host "Restored the original kubelet config on $node."
    exit 0
}

$current = minikube ssh -n $node -- "sudo grep -c 'memory.available' $cfg || true"
if ($current.Trim() -ne "0") {
    # already set (e.g. an older value): change it in place, keep the original backup
    minikube ssh -n $node -- "sudo sed -i 's/^  memory.available: .*/  memory.available: $Threshold/' $cfg && sudo systemctl restart kubelet"
} else {
    minikube ssh -n $node -- "sudo cp $cfg $cfg.bak && sudo sed -i '/^evictionHard:/a\  memory.available: $Threshold' $cfg && sudo sed -i 's/^evictionPressureTransitionPeriod: .*/evictionPressureTransitionPeriod: 60s/' $cfg && sudo systemctl restart kubelet"
}
minikube ssh -n $node -- "sudo grep -A5 evictionHard $cfg; sudo grep evictionPressureTransitionPeriod $cfg"
kubectl wait --for=condition=Ready "node/$node" --timeout=120s
Write-Host "Done: $node evicts when memory.available < $Threshold."
