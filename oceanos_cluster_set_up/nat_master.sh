#!/bin/bash
echo "Enabling ipv4 forwarding (cleaning old rules)"
# flushing old rules -- USE WITH CARE
iptables --flush
iptables --table nat --flush
# MASQUERADE each request form the inside to the outer world
iptables -t nat -A POSTROUTING -j MASQUERADE
# enable IPv4 packet forwarding in the kernel
echo 1 > /proc/sys/net/ipv4/ip_forward
echo "Master is now operating as router"
