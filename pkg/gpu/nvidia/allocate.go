package nvidia

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	log "github.com/golang/glog"
	"golang.org/x/net/context"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	pluginapi "k8s.io/kubernetes/pkg/kubelet/apis/deviceplugin/v1beta1"
)

var (
	clientTimeout    = 30 * time.Second
	lastAllocateTime time.Time
)

// create docker client
func init() {
	kubeInit()
}

// Allocate which return list of devices.
func (m *NvidiaDevicePlugin) Allocate(ctx context.Context,
	reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	responses := pluginapi.AllocateResponse{}

	var (
		podReqGPU uint
		found     bool
		assumePod *v1.Pod
	)
	log.V(6).Infof("Allocate GPU for gpu mem :: ContainerRequests: %v", reqs.ContainerRequests)
	for _, req := range reqs.ContainerRequests {
		podReqGPU += uint(len(req.DevicesIDs))
	}
	log.V(6).Infof("Allocate GPU for gpu mem :: RequestPodGPUs: %d", podReqGPU)

	m.Lock()
	defer m.Unlock()
	log.V(6).Infoln("checking...")
	//获取所有状态为pending的Pod
	candidatePods, err := getCandidatePods(m.queryKubelet, m.kubeletClient)
	if err != nil {
		log.Errorf("invalid allocation requst: failed to find candidate pods due to %v", err)
		return nil, fmt.Errorf("failed to find candidate pods due to %v", err)
	}
	for _, pod := range candidatePods {
		if getGPUMemoryFromPodResource(pod) == podReqGPU {
			log.V(6).Infof("Found GPU shared Pod %s in ns %s with GPU Memory %d",
				pod.Name,
				pod.Namespace,
				podReqGPU)
			assumePod = pod
			found = true
			break
		}
	}
	if !found {
		log.Errorf("not found gpu shared pod need allocate gpu device")
		return nil, fmt.Errorf("not found gpu shared pod need allocate gpu device")
	}
	var devIdx int
	patchData := map[string]interface{}{
		"metadata": map[string]interface{}{
			"annotations": map[string]string{},
			"labels": map[string]string{
				podGpuResourceLabelKey: podGpuResourceLabelValue,
			},
		},
	}
	if isGPUShareAssumedPod(assumePod) {
		devIdx = getGPUIDFromPodAnnotation(assumePod)
		if devIdx < 0 {
			log.Errorf("failed to get dev index for pod %v", assumePod.Name)
			return nil, fmt.Errorf("failed to get dev index")
		}
		// patch 数据
		annotations := patchData["metadata"].(map[string]interface{})["annotations"].(map[string]string)
		annotations[EnvAssignedFlag] = "true"
		annotations[EnvResourceAssumeTime] = fmt.Sprintf("%d", time.Now().UnixNano())
	} else {
		log.V(6).Infof("pod %v unscheduler,directly specify the device", assumePod.Name)
		availableGPUs := m.getAvailableGPUs()
		devIdx = m.assignDevice(availableGPUs, podReqGPU)
		if devIdx < 0 {
			log.Errorf("assign dev for pod %v fail,due to devices unavailable, Requested: %d, Available: %v", assumePod.Name, podReqGPU, availableGPUs)
			return nil, fmt.Errorf("devices unavailable, Requested: %d, Available: %v", podReqGPU, availableGPUs)
		}
		// patch 数据
		annotations := patchData["metadata"].(map[string]interface{})["annotations"].(map[string]string)
		annotations[EnvResourceIndex] = strconv.Itoa(devIdx)
		annotations[EnvResourceByDev] = strconv.Itoa(int(getGPUMemory()))
		annotations[EnvResourceByPod] = strconv.Itoa(int(podReqGPU))
	}
	//1.获取设备序号
	deviceId, ok := m.GetDeviceNameByIndex(uint(devIdx))
	if !ok {
		log.Errorf("failed to find the dev for pod %v because it's not able to find dev with index %d",
			assumePod.Name,
			devIdx)
		return nil, fmt.Errorf("not able to find dev with index %d", devIdx)
	}
	log.V(6).Infof("allocate gpu index %v,uuid: %v", devIdx, deviceId)
	// 2. Create container requests
	for _, req := range reqs.ContainerRequests {
		reqGPU := uint(len(req.DevicesIDs))
		response := pluginapi.ContainerAllocateResponse{
			Envs: map[string]string{
				envNVGPU:               fmt.Sprintf("%v", devIdx),
				EnvResourceIndex:       fmt.Sprintf("%d", devIdx),
				EnvResourceByPod:       fmt.Sprintf("%d", podReqGPU),
				EnvResourceByContainer: fmt.Sprintf("%d", reqGPU),
				EnvResourceByDev:       fmt.Sprintf("%d", getGPUMemory()),
			},
		}
		if m.disableCGPUIsolation {
			response.Envs["CGPU_DISABLE"] = "true"
		}
		responses.ContainerResponses = append(responses.ContainerResponses, &response)
	}
	// 3. Update Pod spec
	err = patchPod(assumePod, patchData)
	if err != nil {
		log.Errorf("patchPod failed, due to %v", err)
		return nil, fmt.Errorf("patchPod failed, due to %v", err)
	}
	log.V(6).Infof("pod %v, new allocated GPUs info %v", assumePod.Name, responses)
	return &responses, nil
}

// 更新Pod信息
func patchPod(pod *v1.Pod, patchData map[string]interface{}) (err error) {
	patchDataBytes, err := json.Marshal(patchData)
	if err != nil {
		return err
	}
	_, err = clientset.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.StrategicMergePatchType, patchDataBytes)
	if err != nil {
		// the object has been modified; please apply your changes to the latest version and try again
		if err.Error() == OptimisticLockErrorMsg {
			// retry
			_, err = clientset.CoreV1().Pods(pod.Namespace).Patch(pod.Name, types.StrategicMergePatchType, patchDataBytes)
		}
	}
	return err
}
