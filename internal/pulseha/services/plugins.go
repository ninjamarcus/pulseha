package services

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"path"
	"path/filepath"
	"plugin"
)

// PluginHC is the health check object structure
type PluginHC interface {
	Name() string
	Version() float64
	Weight() int64
	Run(db *Database) error
	Send() error
}

// PluginNet is the network plugin object structure
type PluginNet interface {
	Name() string
	Version() float64
	BringUpIPs(iface string, ips []string) error
	BringDownIPs(iface string, ips []string) error
}

// PluginGen is the general plugin object structure
type PluginGen interface {
	Name() string
	Version() float64
	Run(db *Database) error
	OnMemberListStatusChange(members []Member)
	OnMemberFailover(member Member)
}

// Plugins object structure which stores our plugins
type Plugins struct {
	modules []*Plugin
}

// Plugin Plugin object structure
type Plugin struct {
	Name    string
	Version float64
	Type    interface{}
	Plugin  interface{}
}

type pluginType int

const (
	PluginHealthCheck pluginType = 1 + iota
	PluginNetworking
	PluginGeneral
)

var pluginTypeNames = []string{
	"PluginHC",
	"PluginNet",
	"PluginGeneral",
}

func (p pluginType) String() string {
	return pluginTypeNames[p-1]
}

// Setup defines each type of plugin to load
func (p *Plugins) Setup() {
	// Define the plugin path pattern
	evtGlob := path.Join("/usr/local/lib/pulseha", "*.so")

	// Retrieve files matching the pattern
	evt, err := filepath.Glob(evtGlob)
	if err != nil {
		log.Fatalf("Failed to retrieve plugins: %v", err)
	}

	// Load plugins
	var plugins []*plugin.Plugin
	for _, pFile := range evt {
		plug, err := plugin.Open(pFile)
		if err != nil {
			log.Warningf("Unable to load plugin %s: possibly out of date. Error: %v", pFile, err)
		} else {
			plugins = append(plugins, plug)
		}
	}

	// Load and validate plugins by category
	p.Load(PluginHealthCheck, plugins)
	p.Load(PluginNetworking, plugins)
	p.Load(PluginGeneral, plugins)
	p.Validate()

	// Log loaded plugins
	if len(p.modules) > 0 {
		var pluginNames string
		for _, plgn := range p.modules {
			pluginNames += fmt.Sprintf("%s(v%.1f) ", plgn.Name, plgn.Version)
		}
		log.Infof("Plugins loaded (%d): %s", len(p.modules), pluginNames)
	} else {
		log.Info("No plugins were loaded.")
	}
}

// Validate ensures that minimal required plugins are loaded.
func (p *Plugins) Validate() {
	// make sure we have a networking plugin
	if p.GetNetworkingPlugin() == nil {
		log.Warning("No networking plugin loaded. PulseHA now in monitoring mode..")
	}
}

// Load loads plugins of a specified type.
func (p *Plugins) Load(pluginType pluginType, pluginList []*plugin.Plugin) {
	for _, plug := range pluginList {
		// Lookup plugin symbol
		sym, err := plug.Lookup(pluginType.String())
		if err != nil {
			log.Debugf("Plugin does not match type %v: %v", pluginType, err)
			continue
		}

		// Call the corresponding plugin handler
		switch pluginType {
		case PluginGeneral:
			p.loadPlugin(sym, pluginType, func(e interface{}) bool {
				plugGen, ok := e.(PluginGen)
				if ok {
					go plugGen.Run(DB)
				}
				return ok
			})

		case PluginHealthCheck:
			p.loadPlugin(sym, pluginType, func(e interface{}) bool {
				plugHC, ok := e.(PluginHC)
				if ok {
					go plugHC.Run(DB)
				}
				return ok
			})

		case PluginNetworking:
			// Ensure only one networking plugin is loaded
			if p.GetNetworkingPlugin() == nil {
				p.loadPlugin(sym, pluginType, func(e interface{}) bool {
					plugNet, ok := e.(PluginNet)
					return ok
				})
			}
		}
	}
}

// loadPlugin loads a plugin if it matches the required type.
func (p *Plugins) loadPlugin(sym interface{}, pluginType pluginType, typeCheck func(interface{}) bool) {
	if typeCheck(sym) {
		plugin := sym.(Plugin)
		newPlugin := &Plugin{
			Name:    plugin.Name(),
			Version: plugin.Version(),
			Type:    pluginType,
			Plugin:  plugin,
		}
		p.modules = append(p.modules, newPlugin)
	}
}

// GetHealthCheckPlugins retrieves all health check plugins.
func (p *Plugins) GetHealthCheckPlugins() []*Plugin {
	return p.getPluginsByType(PluginHealthCheck)
}

// GetNetworkingPlugin retrieves the networking plugin, ensuring only one exists.
func (p *Plugins) GetNetworkingPlugin() *Plugin {
	for _, plgin := range p.modules {
		if plgin.Type == PluginNetworking {
			return plgin
		}
	}
	return nil
}

// GetGeneralPlugins retrieves all general plugins.
func (p *Plugins) GetGeneralPlugins() []*Plugin {
	return p.getPluginsByType(PluginGeneral)
}

// getPluginsByType is a helper that returns a slice of plugins matching a specified type.
func (p *Plugins) getPluginsByType(pluginType pluginType) []*Plugin {
	var plugins []*Plugin
	for _, plgin := range p.modules {
		if plgin.Type == pluginType {
			plugins = append(plugins, plgin)
		}
	}
	return plugins
}
