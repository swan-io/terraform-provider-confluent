package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/hashicorp/go-retryablehttp"
	"log"
	"net/http"
	"strconv"
)

type Config struct {
	ApiEndpoint string
	Email       string
	Password    string
	Me          *Me
	Session     *Session
	AccessToken *AccessToken
}

type Session struct {
	Error interface{} "json:error"
	Token string      "json:token"
	User  User        "json:user"
}

type User struct {
	Id                 int         "json:id"
	Email              string      "json:email"
	FirstName          string      `json:"first_name"`
	LastName           string      `json:"last_name"`
	OrganizationId     int         `json:"organization_id"`
	Deactivated        bool        "json:deactivated"
	Verified           string      "json:verified"
	Created            string      "json:created"
	Modified           string      "json:modified"
	PasswordChanged    string      `json:"password_changed"`
	ServiceName        string      `json:"service_name"`
	ServiceDescription string      `json:"service_description"`
	ServiceAccount     bool        `json:"service_account"`
	Sso                interface{} "json:sso"
	Preferences        interface{} "json:preferences"
	Internal           bool        "json:internal"
}

type Me struct {
	User         User         "json:user"
	Account      Account      "json:account"
	Organization Organization "json:organization"
	Error        interface{}  "json:error"
	Accounts     []Account    "json:accounts"
}

type Account struct {
	Id             string      "json:id"
	Name           string      "json:name"
	OrganizationId int         `json:"organization_id"`
	Deactivated    bool        "json:deactivated"
	Created        string      "json:created"
	Modified       string      "json:modified"
	Config         interface{} "json:config"
	Internal       bool        "json:internal"
}

type Organization struct {
	Id               int         "json:id"
	Name             string      "json:name"
	Deactivated      bool        "json:deactivated"
	StripeCustomerId string      `json:"stripe_customer_id"`
	Created          string      "json:created"
	Modified         string      "json:modified"
	BillingEmail     string      `json:"billing_email"`
	Plan             interface{} "json:plan"
	Saml             interface{} "json:saml"
	Sso              interface{} "json:sso"
	Marketplace      interface{} "json:marketplace"
	ResourceId       string      `json:"resource_id"`
	HasEntitlement   bool        `json:"has_entitlement"`
	ShowBilling      bool        `json:"show_billing"`
	AuditLog         interface{} `json:"audit_log"`
}

type AccessToken struct {
	Error interface{} "json:error"
	Token string      "json:token"
}

type Cluster struct {
	Id                string "json:id"
	Name              string "json:name"
	AccountId         string `json:"account_id"`
	NetworkIngress    int    `json:"network_ingress"`
	NetworkEgress     int    `"json:"network_egress"`
	Storage           int    "json:storage"
	Durability        string "json:durability"
	Status            string "json:status"
	Endpoint          string "json:endpoint"
	Region            string "json:region"
	Created           string "json:created"
	Modified          string "json:modified"
	ServiceProvider   string `json:"service_provider"`
	OrganizationId    int    `json:"organization_id"`
	Enterprise        bool   "json:enterprise"
	K8SClusterId      string `json:"k8s_cluster_id"`
	PhysicalClusterId string `json:"physical_cluster_id"`
	PricePerHourd     string `json:"price_per_hour"`
	AccruedThisCycle  string `json:"accrued_this_cycle"`
	LegacyEndpoint    bool   `json:"legacy_endpoint"`
	Type              string "json:type"
	ApiEndpoint       string `json:"api_endpoint"`
	InternalProxy     bool   `json:"internal_proxy"`
	IsSlaEnabled      bool   `json:"is_sla_enabled"`
	IsSchedulable     bool   `json:"is_schedulable"`
	Dedicated         bool   "json:dedicated"
}

type Clusters struct {
	Error    interface{} "json:error"
	Clusters []Cluster   "json:clusters"
}

type KafkaHost struct {
	Id   int    "json:id"
	Host string "json:host"
	Port int    "json:port"
	Rack int    "json:rack"
}

type KafkaPartition struct {
	Partition int         "json:partition"
	Leader    KafkaHost   "json:leader"
	Replicas  []KafkaHost "json:replicas"
	Isr       []KafkaHost "json:isr" // ??
}

type KafkaTopic struct {
	Name                 string           "json:name"
	Internal             bool             "json:internal"
	AuthorizedOperations []string         "json:authorizedOperations"
	Partitions           []KafkaPartition "json:partitions"
	Configs              []KafkaTopicConfig
}

type KafkaTopicConfig struct {
	Name      string
	Value     string
	ReadOnly  bool
	Sensitive bool
}

type CreateTopicConfig struct {
	CleanUpPolicy     string `json:"cleanup.policy"`
	DeleteRetentionMs string `json:"delete.retention.ms"`
	MaxMessageBytes   string `json:"max.message.bytes"`
	RetentionBytes    string `json:"retention.bytes"`
	RetentionMs       int    `json:"retention.ms"`
}

type CreateTopicRequest struct {
	Name              string            `json:"name""`
	NumPartitions     int               `json:"numPartitions"`
	ReplicationFactor int               `json:"replicationFactor"`
	Configs           CreateTopicConfig `json:"configs""`
}

func (c *Config) loadAndValidate() error {

	return nil
}

func (c *Config) connect() error {
	log.Printf("Connecting")
	if c.Session == nil {
		session, err := getSession(c.ApiEndpoint, c.Email, c.Password)
		if err != nil {
			return err
		}
		c.Session = session
	}
	if c.Me == nil {
		me, err := getMe(c.ApiEndpoint, *c.Session)
		if err != nil {
			return err
		}
		c.Me = me
	}
	if c.AccessToken == nil {
		accessToken, err := getAccessToken(c.ApiEndpoint, *c.Session)
		if err != nil {
			return nil
		}
		c.AccessToken = accessToken
	}
	log.Printf("End Connecting")
	return nil
}

func (c *Config) getClusters() (*Clusters, error) {
	client := http.Client{}
	requestCluster, err := http.NewRequest("GET", c.ApiEndpoint+"/api/clusters?account_id="+c.Me.Account.Id, nil)
	requestCluster.Header.Add("cookie", "auth_token="+c.Session.Token)
	respCluster, err := client.Do(requestCluster)
	if err != nil {
		return nil, err
	}
	defer respCluster.Body.Close()
	if respCluster.StatusCode != 200 {
		return nil, errors.New("HTTP error code getting clusters: " + strconv.Itoa(respCluster.StatusCode))
	}

	var clusters Clusters
	json.NewDecoder(respCluster.Body).Decode(&clusters)
	return &clusters, nil
}

func (c *Config) getCluster(clusterName string) (*Cluster, error) {
	clusters, err := c.getClusters()
	if err != nil {
		return nil, err
	}
	for _, cluster := range clusters.Clusters {
		if cluster.Name == clusterName {
			return &cluster, nil
		}
	}

	return nil, errors.New("Unable to find Cluster with name : " + clusterName)
}

func (c *Config) getTopics(cluster Cluster) ([]KafkaTopic, error) {
	client := http.Client{}
	requestTopics, err := http.NewRequest("GET", cluster.ApiEndpoint+"/2.0/kafka/"+cluster.Id+"/topics", nil)
	requestTopics.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	respTopics, err := client.Do(requestTopics)
	if err != nil {
		return nil, err
	}
	if respTopics.StatusCode != 200 {
		return nil, errors.New("HTTP error code getting Access Token: " + strconv.Itoa(respTopics.StatusCode))
	}
	defer respTopics.Body.Close()

	var kafkaTopics []KafkaTopic
	json.NewDecoder(respTopics.Body).Decode(&kafkaTopics)
	return kafkaTopics, nil
}

func (c *Config) loadTopicConfig(cluster Cluster, topic *KafkaTopic) error {
	log.Printf("Loading topic config")
	client := http.Client{}
	requestTopics, err := http.NewRequest("GET", cluster.ApiEndpoint+"/2.0/kafka/"+cluster.Id+"/topics/"+topic.Name+"/config", nil)
	requestTopics.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	//requestTopics.Header.Set("Content-Type", "application/json")
	respTopics, err := client.Do(requestTopics)
	if err != nil {
		return err
	}
	if respTopics.StatusCode != 200 {
		return errors.New("HTTP error code getting Access Token: " + strconv.Itoa(respTopics.StatusCode))
	}
	defer respTopics.Body.Close()

	var entries map[string][]interface{}
	json.NewDecoder(respTopics.Body).Decode(&entries)

	for _, entry := range entries["entries"] {
		e := entry.(map[string]interface{})
		topic.Configs = append(topic.Configs, KafkaTopicConfig{
			Name:      e["name"].(string),
			Value:     e["value"].(string),
			ReadOnly:  e["isReadOnly"].(bool),
			Sensitive: e["isSensitive"].(bool),
		})
	}
	return nil
}

func (c *Config) getTopic(cluster Cluster, topicName string) (*KafkaTopic, error) {
	topics, err := c.getTopics(cluster)
	if err != nil {
		return nil, err
	}
	for _, topic := range topics {
		if topic.Name == topicName {
			c.loadTopicConfig(cluster, &topic)
			return &topic, nil
		}
	}
	return nil, errors.New("Unable to find Topic with name : " + topicName + " in Cluster " + cluster.Name)
}

func CreateTopicRetryPolicy(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if resp.StatusCode == 0 || (resp.StatusCode == 400) {
		log.Printf("Unable to create topic, retrying ...")
		return true, nil
	}
	return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
}

func getCreateTopicConfig(params []KafkaTopicConfig, paramName string) string {
	for _, param := range params {
		if param.Name == paramName {
			return param.Value
		}
	}
	return ""
}

func (c *Config) createTopic(cluster Cluster, name string, numPartitions int, params []KafkaTopicConfig) error {
	//client := http.Client{}
	client := retryablehttp.NewClient()
	client.CheckRetry = CreateTopicRetryPolicy

	retentionMs, _ := strconv.Atoi(getCreateTopicConfig(params, "retention.ms"))
	config := CreateTopicRequest{
		Name:              name,
		NumPartitions:     numPartitions,
		ReplicationFactor: 3,
		Configs: CreateTopicConfig{
			CleanUpPolicy:     getCreateTopicConfig(params, "cleanup.policy"),
			DeleteRetentionMs: getCreateTopicConfig(params, "delete.retention.ms"),
			MaxMessageBytes:   getCreateTopicConfig(params, "max.message.bytes"),
			RetentionBytes:    getCreateTopicConfig(params, "retention.bytes"),
			RetentionMs:       retentionMs,
		},
	}
	bytesRepresentation, err := json.Marshal(config)
	requestCreateTopic, err := retryablehttp.NewRequest("PUT", cluster.ApiEndpoint+"/2.0/kafka/"+cluster.Id+"/topics?validate=false", bytes.NewBuffer(bytesRepresentation))
	requestCreateTopic.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	requestCreateTopic.Header.Set("Content-Type", "application/json")
	respTopics, err := client.Do(requestCreateTopic)
	if err != nil {
		return err
	}
	if respTopics.StatusCode != 204 {
		return errors.New("HTTP error code creating Topic : " + strconv.Itoa(respTopics.StatusCode))
	}

	errUpdateTopic := c.updateTopicConfig(cluster, name, params)
	if errUpdateTopic != nil {
		return errUpdateTopic
	}

	return nil
}

func (c *Config) updateTopicConfig(cluster Cluster, topicName string, params []KafkaTopicConfig) error {
	client := retryablehttp.NewClient()
	var configs []map[string]interface{}

	for _, param := range params {
		log.Printf(param.Name + "=" + param.Value)
		configs = append(configs, map[string]interface{}{
			"name":  param.Name,
			"value": param.Value,
		})
	}
	topicConfig := map[string]interface{}{
		"entries": configs,
	}
	bytesRepresentation, err := json.Marshal(topicConfig)
	log.Printf(bytes.NewBuffer(bytesRepresentation).String())
	requestUpdateTopicConfig, err := retryablehttp.NewRequest("PUT", cluster.ApiEndpoint+"/2.0/kafka/"+cluster.Id+"/topics/"+topicName+"/config", bytes.NewBuffer(bytesRepresentation))
	requestUpdateTopicConfig.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	requestUpdateTopicConfig.Header.Set("Content-Type", "application/json")
	respTopics, err := client.Do(requestUpdateTopicConfig)
	if err != nil {
		return err
	}
	if respTopics.StatusCode != 204 {
		return errors.New("HTTP error code updating Topic : " + strconv.Itoa(respTopics.StatusCode))
	}
	return nil
}

func (c *Config) deleteTopic(cluster Cluster, topicName string) error {
	client := http.Client{}
	requestDeleteTopic, err := http.NewRequest("DELETE", cluster.ApiEndpoint+"/2.0/kafka/"+cluster.Id+"/topics/"+topicName, nil)
	requestDeleteTopic.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	respDeleteTopic, err := client.Do(requestDeleteTopic)
	if err != nil {
		return err
	}
	if respDeleteTopic.StatusCode != 204 {
		return errors.New("HTTP error code deleting Topic : " + strconv.Itoa(respDeleteTopic.StatusCode))
	}
	return nil
}

func getSession(apiEndpoint string, login string, password string) (*Session, error) {
	message := map[string]interface{}{
		"email":    login,
		"password": password,
	}
	bytesRepresentation, err := json.Marshal(message)
	if err != nil {
		return nil, err
	}

	client := http.Client{}
	request, err := http.NewRequest("POST", apiEndpoint+"/api/sessions", bytes.NewBuffer(bytesRepresentation))
	request.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(request)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, errors.New("HTTP error code while login: " + strconv.Itoa(resp.StatusCode))
	}
	defer resp.Body.Close()

	var session Session
	json.NewDecoder(resp.Body).Decode(&session)

	return &session, nil
}

func getMe(apiEndpoint string, session Session) (*Me, error) {
	client := http.Client{}
	requestMe, err := http.NewRequest("GET", apiEndpoint+"/api/me", nil)
	requestMe.Header.Add("cookie", "auth_token="+session.Token)
	respMe, err := client.Do(requestMe)
	if err != nil {
		return nil, err
	}
	defer respMe.Body.Close()
	if respMe.StatusCode != 200 {
		return nil, errors.New("HTTP error code getting me: " + strconv.Itoa(respMe.StatusCode))
	}

	var me Me
	json.NewDecoder(respMe.Body).Decode(&me)
	return &me, nil
}

func getAccessToken(apiEndpoint string, session Session) (*AccessToken, error) {
	client := http.Client{}
	emptyBody, err := json.Marshal(map[string]int{})
	requestAccessToken, err := http.NewRequest("POST", apiEndpoint+"/api/access_tokens", bytes.NewBuffer(emptyBody))
	requestAccessToken.Header.Set("Content-Type", "application/json")
	requestAccessToken.Header.Add("cookie", "auth_token="+session.Token)
	respAccessToken, err := client.Do(requestAccessToken)
	if err != nil {
		return nil, err
	}
	defer respAccessToken.Body.Close()
	if respAccessToken.StatusCode != 200 {
		return nil, errors.New("HTTP error code getting Access Token: " + strconv.Itoa(respAccessToken.StatusCode))
	}

	var accessToken AccessToken
	json.NewDecoder(respAccessToken.Body).Decode(&accessToken)
	return &accessToken, nil
}
