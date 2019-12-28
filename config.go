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
	"strings"
	"sync"
)

type Config struct {
	ApiEndpoint string
	Email       string
	Password    string
	Me          *Me
	Session     *Session
	AccessToken *AccessToken
	Mutex       sync.Mutex
}

type Session struct {
	Error interface{} "json:error"
	Token string      "json:token"
	User  User        "json:user"
}

type User struct {
	Id                 int         `json:"id"`
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

func (cluster *Cluster) Host() string {
	if cluster.Endpoint != "" {
		tokens := strings.Split(cluster.Endpoint, ":")
		return tokens[1][2:len(tokens[1])]
	}
	return ""
}

func (cluster *Cluster) Protocol() string {
	if cluster.Endpoint != "" {
		return strings.Split(cluster.Endpoint, ":")[0]
	}
	return ""
}

func (cluster *Cluster) Port() int {
	if cluster.Endpoint != "" {
		if port, err := strconv.Atoi(strings.Split(cluster.Endpoint, ":")[2]); err != nil {
			return 0
		} else {
			return port
		}

	}
	return 0
}

type ApiKey struct {
	Id              int           `json:"Id"`
	Key             string        `json:"key"`
	Secret          string        `json:"secret"`
	HashSecret      string        `json:"hashed_secret"`
	HashFunction    string        `json:"hash_function"`
	SaslMechanism   string        `json:"sasl_mechanism"`
	UserId          int           `json:"user_id"`
	Deactivated     bool          `json:"deactivated"`
	Created         string        `json:"created"`
	Modified        string        `json:"modified"`
	Description     string        `json:"description"`
	Internal        interface{}   `json:"internal"`
	LogicalClusters []interface{} `json:"logical_clusters"`
	AccountId       string        `json:"account_id"`
	ServiceAccount  bool          `json:"service_account"`
}

type ApiKeysResponse struct {
	ApiKeys []ApiKey    `json:"api_keys"`
	Error   interface{} `json:"error"`
}

type ApiKeyResponse struct {
	ApiKey ApiKey      `json:"api_key"`
	Error  interface{} `json:"error"`
}

type CreateClusterResponse struct {
	Error            interface{} `json:"error"`
	ValidationErrors interface{} `json:"validation_errors"`
	Cluster          Cluster     `json:"cluster"`
	Credentials      interface{} `json:"credentials"`
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

type ApiKeyRequest struct {
	AccountId       string                  `json:"account_id"`
	LogicalClusters []LogicalClusterRequest `json:"logical_clusters"`
}

type ApiKeyRequestDelete struct {
	Id              int                     `json:"id"`
	AccountId       string                  `json:"account_id"`
	LogicalClusters []LogicalClusterRequest `json:"logical_clusters"`
}

type LogicalClusterRequest struct {
	Id string `json:"id"`
}

type CreateApiKeyRequest struct {
	ApiKey ApiKeyRequest `json:"api_key"`
}

type DeleteApiKeyRequest struct {
	ApiKey ApiKeyRequestDelete `json:"api_key"`
}

type CreateApiKeyResponse struct {
	ApiKey ApiKey      `json:"api_key"`
	Error  interface{} `json:"error"`
}

type GetApiKeysResponse struct {
	ApiKeys []ApiKey    `json:"api_keys"`
	Error   interface{} `json:"error"`
}

func (c *Config) loadAndValidate() error {

	return nil
}

func (c *Config) connect() error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
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
	return c.getClustersPerAccount(c.Me.Account.Id)
}

func (c *Config) getClustersPerAccount(accountId string) (*Clusters, error) {
	client := http.Client{}
	requestCluster, err := http.NewRequest("GET", c.ApiEndpoint+"/api/clusters?account_id="+accountId, nil)
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

func (c *Config) createClusterPerAccount(accountId string, name string, durability string, region string, serviceProvider string) (*Cluster, error) {
	createClusterRequest := map[string]map[string]interface{}{
		"config": {
			"name":             name,
			"account_id":       accountId,
			"network_ingress":  100,
			"network_egress":   100,
			"storage":          5000,
			"durability":       durability,
			"region":           region,
			"service_provider": serviceProvider,
		},
	}

	bytesRepresentation, err := json.Marshal(createClusterRequest)
	if err != nil {
		return nil, err
	}
	//log.Printf(bytes.NewBuffer(bytesRepresentation).String())
	client := retryablehttp.NewClient()
	requestCreateCluster, err := retryablehttp.NewRequest("POST", "https://confluent.cloud/api/clusters", bytes.NewBuffer(bytesRepresentation))
	if err != nil {
		return nil, err
	}
	requestCreateCluster.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	requestCreateCluster.Header.Set("Content-Type", "application/json")
	responseClusterCreate, err := client.Do(requestCreateCluster)
	if err != nil {
		return nil, err
	}
	defer responseClusterCreate.Body.Close()

	if responseClusterCreate.StatusCode != 200 {
		return nil, errors.New("HTTP error code creating cluster : " + strconv.Itoa(responseClusterCreate.StatusCode))
	}

	var CreateClusterResponse CreateClusterResponse
	json.NewDecoder(responseClusterCreate.Body).Decode(&CreateClusterResponse)
	return &CreateClusterResponse.Cluster, nil
}

func (c *Config) deleteCluster(cluster Cluster) error {
	deleteClusterRequest := map[string]map[string]interface{}{
		"cluster": {
			"id":               cluster.Id,
			"name":             cluster.Name,
			"account_id":       cluster.AccountId,
			"network_ingress":  cluster.NetworkIngress,
			"network_egress":   cluster.NetworkEgress,
			"storage":          cluster.Storage,
			"durability":       cluster.Durability,
			"region":           cluster.Region,
			"service_provider": cluster.ServiceProvider,
			"organization_id":  cluster.OrganizationId,
		},
	}
	bytesRepresentation, err := json.Marshal(deleteClusterRequest)
	if err != nil {
		return err
	}
	log.Printf(bytes.NewBuffer(bytesRepresentation).String())
	client := retryablehttp.NewClient()
	requestDeleteCluster, err := retryablehttp.NewRequest("DELETE", "https://confluent.cloud/api/clusters/"+cluster.Id, bytes.NewBuffer(bytesRepresentation))
	if err != nil {
		return err
	}
	requestDeleteCluster.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	requestDeleteCluster.Header.Set("Content-Type", "application/json")
	responseClusterCreate, err := client.Do(requestDeleteCluster)
	if err != nil {
		return err
	}
	defer responseClusterCreate.Body.Close()

	if responseClusterCreate.StatusCode != 200 {
		return errors.New("HTTP error code deleting cluster : " + strconv.Itoa(responseClusterCreate.StatusCode))
	}

	return nil
}

func (c *Config) updateCluster(cluster Cluster, newName string) (*Cluster, error) {
	updateClusterRequest := map[string]map[string]interface{}{
		"cluster": {
			"id":               cluster.Id,
			"name":             newName,
			"account_id":       cluster.AccountId,
			"network_ingress":  cluster.NetworkIngress,
			"network_egress":   cluster.NetworkEgress,
			"storage":          cluster.Storage,
			"durability":       cluster.Durability,
			"region":           cluster.Region,
			"service_provider": cluster.ServiceProvider,
			"organization_id":  cluster.OrganizationId,
		},
	}
	bytesRepresentation, err := json.Marshal(updateClusterRequest)
	if err != nil {
		return nil, err
	}
	log.Printf(bytes.NewBuffer(bytesRepresentation).String())
	client := retryablehttp.NewClient()
	requestUpdateCluster, err := retryablehttp.NewRequest("PUT", "https://confluent.cloud/api/clusters/"+cluster.Id, bytes.NewBuffer(bytesRepresentation))
	if err != nil {
		return nil, err
	}
	requestUpdateCluster.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	requestUpdateCluster.Header.Set("Content-Type", "application/json")
	responseUpdateCluster, err := client.Do(requestUpdateCluster)
	if err != nil {
		return nil, err
	}
	defer responseUpdateCluster.Body.Close()

	if responseUpdateCluster.StatusCode != 200 {
		return nil, errors.New("HTTP error code updating cluster : " + strconv.Itoa(responseUpdateCluster.StatusCode))
	}

	var UpdateClusterResponse CreateClusterResponse
	json.NewDecoder(responseUpdateCluster.Body).Decode(&UpdateClusterResponse)
	return &UpdateClusterResponse.Cluster, nil
}

func (c *Config) getApiKey(cluster Cluster, keyId int) (*ApiKey, error) {
	if apiKeys, err := c.getApiKeys(cluster); err != nil {
		return nil, err
	} else {
		for _, apiKey := range apiKeys {
			log.Printf("Loading key " + strconv.Itoa(apiKey.Id))
			if apiKey.Id == keyId {
				return &apiKey, nil
			}
		}
		log.Printf("Unable to find Api Key with ID " + strconv.Itoa(keyId))
		return nil, nil // Not Found
	}
}

func (c *Config) getApiKeys(cluster Cluster) ([]ApiKey, error) {
	client := retryablehttp.NewClient()
	requestListApiKeys, err := retryablehttp.NewRequest("GET", "https://confluent.cloud/api/api_keys?account_id="+cluster.AccountId+"&cluster_id="+cluster.Id, nil)
	if err != nil {
		return nil, err
	}
	requestListApiKeys.Header.Add("cookie", "auth_token="+c.Session.Token)
	respApiKeys, err := client.Do(requestListApiKeys)
	if err != nil {
		return nil, err
	}
	if respApiKeys.StatusCode != 200 {
		return nil, errors.New("HTTP error code getting API Keys: " + strconv.Itoa(respApiKeys.StatusCode))
	}
	defer respApiKeys.Body.Close()

	var GetApiKeysResponse GetApiKeysResponse
	json.NewDecoder(respApiKeys.Body).Decode(&GetApiKeysResponse)
	log.Printf("API Keys: " + strconv.Itoa(len(GetApiKeysResponse.ApiKeys)))
	return GetApiKeysResponse.ApiKeys, nil
}

func (c *Config) createApiKey(cluster Cluster) (*ApiKey, error) {
	CreateApiKeyRequest := CreateApiKeyRequest{
		ApiKey: ApiKeyRequest{
			AccountId: cluster.AccountId,
			LogicalClusters: []LogicalClusterRequest{
				{Id: cluster.Id},
			},
		},
	}

	bytesRepresentation, err := json.Marshal(CreateApiKeyRequest)
	if err != nil {
		return nil, err
	}
	log.Printf(bytes.NewBuffer(bytesRepresentation).String())
	client := retryablehttp.NewClient()
	requestCreateApiKey, err := retryablehttp.NewRequest("POST", "https://confluent.cloud/api/api_keys", bytes.NewBuffer(bytesRepresentation))
	if err != nil {
		return nil, err
	}
	requestCreateApiKey.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	requestCreateApiKey.Header.Set("Content-Type", "application/json")
	responseCreateApiKey, err := client.Do(requestCreateApiKey)
	if err != nil {
		return nil, err
	}
	defer responseCreateApiKey.Body.Close()

	if responseCreateApiKey.StatusCode != 200 {
		return nil, errors.New("HTTP error code creating API Key : " + strconv.Itoa(responseCreateApiKey.StatusCode))
	}

	var CreateApiKeyResponse CreateApiKeyResponse
	json.NewDecoder(responseCreateApiKey.Body).Decode(&CreateApiKeyResponse)
	return &CreateApiKeyResponse.ApiKey, nil
}

func (c *Config) deleteApiKey(cluster Cluster, keyId int) error {
	DeleteApiKeyRequest := DeleteApiKeyRequest{
		ApiKey: ApiKeyRequestDelete{
			Id:        keyId,
			AccountId: cluster.AccountId,
			LogicalClusters: []LogicalClusterRequest{
				{Id: cluster.Id},
			},
		},
	}

	bytesRepresentation, err := json.Marshal(DeleteApiKeyRequest)
	if err != nil {
		return err
	}
	log.Printf(bytes.NewBuffer(bytesRepresentation).String())
	client := retryablehttp.NewClient()
	requestDeleteApiKey, err := retryablehttp.NewRequest("DELETE", "https://confluent.cloud/api/api_keys/"+strconv.Itoa(keyId), bytes.NewBuffer(bytesRepresentation))
	if err != nil {
		return err
	}
	requestDeleteApiKey.Header.Set("Authorization", "Bearer "+c.AccessToken.Token)
	requestDeleteApiKey.Header.Set("Content-Type", "application/json")
	responseDeleteApiKey, err := client.Do(requestDeleteApiKey)
	if err != nil {
		return err
	}
	defer responseDeleteApiKey.Body.Close()

	if responseDeleteApiKey.StatusCode != 200 {
		return errors.New("HTTP error code deleting API Key : " + strconv.Itoa(responseDeleteApiKey.StatusCode))
	}

	return nil
}

func (c *Config) getClusterPerAccount(accountId string, clusterId string) (*Cluster, error) {
	clusters, err := c.getClustersPerAccount(accountId)
	if err != nil {
		return nil, err
	}
	for _, cluster := range clusters.Clusters {
		if cluster.Id == clusterId {
			return &cluster, nil
		}
	}

	return nil, errors.New("Unable to find Cluster with Id " + clusterId + " for account " + accountId)
}

func (c *Config) getClusterPerAccountAndName(accountId string, clusterName string) (*Cluster, error) {
	clusters, err := c.getClustersPerAccount(accountId)
	if err != nil {
		return nil, err
	}
	for _, cluster := range clusters.Clusters {
		if cluster.Name == clusterName {
			return &cluster, nil
		}
	}

	return nil, errors.New("Unable to find Cluster with Name " + clusterName + " for account " + accountId)
}

func (c *Config) getCluster(accountId string, clusterName string) (*Cluster, error) {
	clusters, err := c.getClustersPerAccount(accountId)
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
