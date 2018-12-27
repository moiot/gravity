package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ConfigFileKey = "config.json"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Pipeline is a specification for a Pipeline resource
type DrcPipeline struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DrcPipelineSpec   `json:"spec"`
	Status DrcPipelineStatus `json:"status"`
}

// PipelineSpec is the spec for a Pipeline resource
type DrcPipelineSpec struct {
	Task       `json:",inline"`
	Paused     bool        `json:"paused"`
	LastUpdate metav1.Time `json:"lastUpdate"`
}

// PipelineStatus is the status for a Pipeline resource
type DrcPipelineStatus struct {
	ObservedGeneration int64 `json:"observedGeneration"`
	Task               `json:",inline"`
	Position           string              `json:"position"`
	PodName            string              `json:"podName"`
	Conditions         []PipelineCondition `json:"conditions,omitempty"`
}

func (t DrcPipelineStatus) Available() bool {
	c := t.Condition(PipelineConditionRunning)
	return c != nil && c.Status == corev1.ConditionTrue
}

func (t DrcPipelineStatus) Condition(condType PipelineConditionType) *PipelineCondition {
	for _, cond := range t.Conditions {
		if cond.Type == condType {
			return &cond
		}
	}

	return nil
}

type Task struct {
	ConfigHash string   `json:"configHash"`
	Image      string   `json:"image"`
	Command    []string `json:"command"`
}

type PipelineConditionType string

const (
	PipelineConditionRunning     PipelineConditionType = "Running"
	PipelineConditionIncremental PipelineConditionType = "Incremental"
)

type PipelineCondition struct {
	// Type of cluster condition.
	Type PipelineConditionType `json:"type"`
	// Status of the condition, one of True, False, Unknown.
	Status corev1.ConditionStatus `json:"status"`
	// LastUpdateTime is the last time this condition was updated.
	LastUpdateTime metav1.Time `json:"lastUpdateTime,omitempty"`
	// LastTransitionTime is the last time the condition transitioned from one status to another.
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty"`
	// Reason for the condition's last transition.
	Reason string `json:"reason,omitempty"`
	// Message which is human readable indicating details about the transition.
	Message string `json:"message,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PipelineList is a list of Pipeline resources
type DrcPipelineList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata"`

	Items []DrcPipeline `json:"items"`
}
