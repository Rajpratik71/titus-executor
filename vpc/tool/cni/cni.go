package cni

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Netflix/titus-executor/vpc/tool/container2"
	"github.com/apparentlymart/go-cidr/cidr"

	"github.com/Netflix/titus-executor/logger"
	vpcapi "github.com/Netflix/titus-executor/vpc/api"
	"github.com/Netflix/titus-executor/vpc/tool/identity"
	"github.com/Netflix/titus-executor/vpc/tracehelpers"
	vpctypes "github.com/Netflix/titus-executor/vpc/types"
	"github.com/containernetworking/cni/pkg/skel"
	"github.com/containernetworking/cni/pkg/types"
	"github.com/containernetworking/cni/pkg/types/current"
	"github.com/containernetworking/cni/pkg/version"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"go.opencensus.io/trace"
	"google.golang.org/grpc"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	//	"github.com/containernetworking/plugins/pkg/utils/sysctl"
)

// These are the names of the annotations we use on the pod to configure
const (
	securityGroupsAnnotations = "com.netflix.titus.network/securityGroups"
)

const (
	maxAllocationIndex = 10240
)

var VersionInfo = version.PluginSupports("0.3.0", "0.3.1")

type Command struct {
	// Never use this context except to get the initial context for Add / Check / Del
	ctx context.Context
	iip identity.InstanceIdentityProvider
	gsv GetSharedValues

	boltDBPath string
}

type config struct {
	k8sArgs K8sArgs
	cfg     TitusCNIConfig

	instanceIdentity *vpcapi.InstanceIdentity

	// Everything is built around the pod
	pod *corev1.Pod

	conn *grpc.ClientConn
}

type GetSharedValues func(ctx context.Context) (*grpc.ClientConn, error)

func MakeCommand(ctx context.Context, instanceIdentityProvider identity.InstanceIdentityProvider, gsv GetSharedValues, boltDBPath string) *Command {
	return &Command{
		ctx:        ctx,
		iip:        instanceIdentityProvider,
		gsv:        gsv,
		boltDBPath: boltDBPath,
	}
}

// Borrowed from: https://github.com/Tanujparihar/aws/blob/87052b192d468fab20bbf4c10590dc2a39885680/plugins/routed-eni/cni.go
type K8sArgs struct {
	types.CommonArgs

	// K8S_POD_NAME is pod's name
	K8S_POD_NAME types.UnmarshallableString // nolint:golint

	// K8S_POD_NAMESPACE is pod's namespace
	K8S_POD_NAMESPACE types.UnmarshallableString // nolint:golint

	// K8S_POD_INFRA_CONTAINER_ID is pod's container id
	K8S_POD_INFRA_CONTAINER_ID types.UnmarshallableString // nolint:golint
}

type TitusCNIConfig struct {
	types.NetConf

	KubeletAPIURL string `json:"KubeletAPIURL"`
}

func getKey(pod *corev1.Pod) string {
	return fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
}

func (c *Command) load(ctx context.Context, args *skel.CmdArgs) (*config, error) {
	ctx, span := trace.StartSpan(ctx, "load")
	defer span.End()
	// Does all the work of loading the config

	retCfg := &config{}

	err := types.LoadArgs(args.Args, &retCfg.k8sArgs)
	if err != nil {
		err = errors.Wrap(err, "Unable to parse CNI args")
		tracehelpers.SetStatus(err, span)
		return nil, err
	}

	retCfg.conn, err = c.gsv(ctx)
	if err != nil {
		tracehelpers.SetStatus(err, span)
		return nil, err
	}

	err = json.Unmarshal(args.StdinData, &retCfg.cfg)
	if err != nil {
		err = errors.Wrap(err, "Cannot parse Kubernetes configuration")
		tracehelpers.SetStatus(err, span)
		return nil, err
	}

	retCfg.instanceIdentity, err = c.iip.GetIdentity(ctx)
	if err != nil {
		err = errors.Wrap(err, "Cannot retrieve instance identity")
		tracehelpers.SetStatus(err, span)
		return nil, err
	}

	// Borrowed from: https://gist.github.com/nownabe/4345d9b68f323ba30905c9dfe3460006

	// https://godoc.org/k8s.io/apimachinery/pkg/runtime#Scheme
	scheme := runtime.NewScheme()

	// https://godoc.org/k8s.io/apimachinery/pkg/runtime/serializer#CodecFactory
	codecFactory := serializer.NewCodecFactory(scheme)

	// https://godoc.org/k8s.io/apimachinery/pkg/runtime#Decoder
	deserializer := codecFactory.UniversalDeserializer()

	// Borrowed from: https://stackoverflow.com/questions/12122159/how-to-do-a-https-request-with-bad-certificate
	customTransport := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, // nolint:gosec
	}

	client := &http.Client{Transport: customTransport}

	ret, err := client.Get(retCfg.cfg.KubeletAPIURL)
	if err != nil {
		err = errors.Wrapf(err, "Cannot fetch pod pod list from kubelet at %s", retCfg.cfg.KubeletAPIURL)
		tracehelpers.SetStatus(err, span)
		return nil, err
	}
	defer ret.Body.Close()

	body, err := ioutil.ReadAll(ret.Body)
	if err != nil {
		err = errors.Wrap(err, "Cannot read body from Kubelet")
		tracehelpers.SetStatus(err, span)
		return nil, err
	}

	podListObject, _, err := deserializer.Decode(body, nil, &corev1.PodList{})
	if err != nil {
		err = errors.Wrap(err, "Cannot deserialize podlist from kubelet")
		tracehelpers.SetStatus(err, span)
		return nil, err
	}

	// I think this works?
	podList := podListObject.(*corev1.PodList)

	namespace := string(retCfg.k8sArgs.K8S_POD_NAMESPACE)
	name := string(retCfg.k8sArgs.K8S_POD_NAME)
	for idx := range podList.Items {
		pod := podList.Items[idx]
		if pod.Namespace == namespace && pod.Name == name {
			retCfg.pod = &pod
			return retCfg, nil
		}
	}

	err = fmt.Errorf("Could not find pod %s, in namespace %s", name, namespace)
	tracehelpers.SetStatus(err, span)
	return nil, err
}

func allocateIndex(ctx context.Context, boltDBPath string, pod *corev1.Pod) (idx uint16, retErr error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ctx, span := trace.StartSpan(ctx, "allocateIndex")
	defer span.End()
	_ = ctx

	db, err := bolt.Open(boltDBPath, 0644, &bolt.Options{Timeout: 10 * time.Second})
	if err != nil {
		err = errors.Wrap(err, "Cannot open bolt DB")
		tracehelpers.SetStatus(err, span)
		return 0, err
	}
	defer func() {
		if retErr == nil {
			retErr = db.Close()
			if retErr != nil {
				retErr = errors.Wrap(err, "Cannot close bolt DB")
				tracehelpers.SetStatus(err, span)
			}
		}
	}()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	podKey := []byte(getKey(pod))

	tx, err := db.Begin(true)
	if err != nil {
		err = errors.Wrap(err, "Cannot open bolt DB")
		tracehelpers.SetStatus(err, span)
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	podToKeyBucket, err := tx.CreateBucketIfNotExists([]byte("pod2key"))
	if err != nil {
		err = errors.Wrap(err, "Cannot create / get pod to key bucket")
		tracehelpers.SetStatus(err, span)
		return 0, err
	}

	keyToPodBucket, err := tx.CreateBucketIfNotExists([]byte("key2pod"))
	if err != nil {
		err = errors.Wrap(err, "Cannot create / get pod to key bucket")
		tracehelpers.SetStatus(err, span)
		return 0, err
	}

	maybeKey := podToKeyBucket.Get(podKey)
	if maybeKey != nil {
		val, err := strconv.Atoi(string(maybeKey))
		return uint16(val), err
	}

	for i := r.Intn(maxAllocationIndex); i < 2*maxAllocationIndex; i++ {
		val := (i % (maxAllocationIndex - 3)) + 3
		idxKey := []byte(strconv.Itoa(val))
		maybePod := keyToPodBucket.Get(idxKey)
		if maybePod == nil {
			err = keyToPodBucket.Put(idxKey, podKey)
			if err != nil {
				err = errors.Wrap(err, "Cannot put value in key to pod bucket")
				tracehelpers.SetStatus(err, span)
				return 0, err
			}
			err = podToKeyBucket.Put(podKey, idxKey)
			if err != nil {
				err = errors.Wrap(err, "Cannot put value in key to pod bucket")
				tracehelpers.SetStatus(err, span)
				return 0, err
			}
			err = tx.Commit()
			if err != nil {
				err = errors.Wrap(err, "Cannot commit database transaction")
				tracehelpers.SetStatus(err, span)
				return 0, err
			}
			return uint16(val), nil
		}
	}

	return 0, errors.New("Could not generate lock for index")

}

func (c *Command) Add(args *skel.CmdArgs) error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()
	ctx, span := trace.StartSpan(ctx, "Add")
	defer span.End()

	cfg, err := c.load(ctx, args)
	if err != nil {
		tracehelpers.SetStatus(err, span)
		return err
	}

	// TODO:
	// 1. Add sysctls
	// 2. Add "extrahosts"
	// 3. Configure DAD

	// Extra options we want / need
	// IPv6
	// Static IP(s)
	// Subnets
	// Account ID

	securityGroups := cfg.pod.Annotations[securityGroupsAnnotations]
	span.AddAttributes(trace.StringAttribute("securityGroups", securityGroups))
	securityGroupsList := strings.Split(securityGroups, ",")

	ns, err := os.Open(args.Netns)
	if err != nil {
		err = errors.Wrapf(err, "Cannot open container netns: %s", args.Netns)
		tracehelpers.SetStatus(err, span)
		return err
	}
	defer ns.Close()

	allocationIndex, err := allocateIndex(ctx, c.boltDBPath, cfg.pod)
	if err != nil {
		tracehelpers.SetStatus(err, span)
		return err
	}

	assignIPRequest := &vpcapi.AssignIPRequestV3{
		InstanceIdentity: cfg.instanceIdentity,
		TaskId:           getKey(cfg.pod),
		SecurityGroupIds: securityGroupsList,
		Ipv4:             &vpcapi.AssignIPRequestV3_Ipv4AddressRequested{Ipv4AddressRequested: true},
	}

	client := vpcapi.NewTitusAgentVPCServiceClient(cfg.conn)

	response, err := client.AssignIPV3(ctx, assignIPRequest)
	if err != nil {
		logger.G(ctx).WithError(err).Error("AssignIP request failed")
		tracehelpers.SetStatus(err, span)
		return err
	}

	alloc := vpctypes.Allocation{
		Success:         true,
		BranchENIID:     response.BranchNetworkInterface.NetworkInterfaceId,
		BranchENIMAC:    response.BranchNetworkInterface.MacAddress,
		BranchENIVPC:    response.BranchNetworkInterface.VpcId,
		BranchENISubnet: response.BranchNetworkInterface.SubnetId,
		VlanID:          int(response.VlanId),
		TrunkENIID:      response.TrunkNetworkInterface.NetworkInterfaceId,
		TrunkENIMAC:     response.TrunkNetworkInterface.MacAddress,
		TrunkENIVPC:     response.TrunkNetworkInterface.VpcId,
		DeviceIndex:     int(response.VlanId),
		AllocationIndex: allocationIndex,
	}

	if response.Ipv6Address != nil {
		alloc.IPV6Address = response.Ipv6Address
	}

	if response.Ipv4Address != nil {
		alloc.IPV4Address = response.Ipv4Address
	}

	logger.G(ctx).WithField("response", response.String()).WithField("allocation", fmt.Sprintf("%+v", alloc)).Info("Allocated IP")

	mask := net.CIDRMask(int(response.Ipv4Address.PrefixLength), 32)
	ip := net.ParseIP(response.Ipv4Address.Address.Address)
	ipnet := net.IPNet{IP: ip, Mask: mask}
	zeroIdx := 0
	gateway := cidr.Inc(ip.Mask(mask))
	logger.G(ctx).WithField("gateway", gateway).Debug("Adding default route")

	_, err = container2.DoSetupContainer(ctx, int(ns.Fd()), 128000, 1280000, false, alloc)
	if err != nil {
		err = errors.Wrap(err, "Cannot not setup network")
		tracehelpers.SetStatus(err, span)
		return err
	}

	result := current.Result{
		CNIVersion: "0.3.1",
		Interfaces: []*current.Interface{
			{
				Name:    "eth0",
				Mac:     response.BranchNetworkInterface.MacAddress,
				Sandbox: args.Netns,
			},
		},
		IPs: []*current.IPConfig{
			{
				Version:   "4",
				Interface: &zeroIdx,
				Address:   ipnet,
				Gateway:   gateway,
			},
		},
		Routes: []*types.Route{
			{
				Dst: net.IPNet{
					IP:   net.ParseIP("0.0.0.0"),
					Mask: net.CIDRMask(0, 32),
				},
				GW: gateway,
			},
		},
		DNS: types.DNS{
			// TODO
			Nameservers: []string{"169.254.169.253"},
			Domain:      "",
			Search:      nil,
			Options:     []string{"edns0", "timeout:2", "rotate"},
		},
	}

	return types.PrintResult(&result, cfg.cfg.CNIVersion)
}

func (c *Command) Check(_ *skel.CmdArgs) error {
	return nil
}

func (c *Command) Del(args *skel.CmdArgs) error {
	ctx, cancel := context.WithCancel(c.ctx)
	defer cancel()

	ctx, span := trace.StartSpan(ctx, "Del")
	defer span.End()

	logger.G(ctx).WithField("args", args).Info("Deleting Networking")

	return errors.New("Not yet implemented")
}

func stableSecret() string {
	ipBuf := make([]byte, 16)
	// We can use math/rand here because this doesn't have to be cryptographically secure
	s := rand.NewSource(time.Now().UnixNano())
	r := rand.New(s)
	n, err := r.Read(ipBuf) // nolint: gosec
	if err != nil {
		panic(err)
	}
	if n != len(ipBuf) {
		panic(fmt.Sprintf("rand.Read only read %d bytes, not %d bytes", n, len(ipBuf)))
	}
	return net.IP(ipBuf).String()
}
