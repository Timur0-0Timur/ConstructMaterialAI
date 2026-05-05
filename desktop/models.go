package main

type Equipment struct {
	Type     string `json:"type"`
	Tag      string `json:"tag"`
	Quantity int    `json:"quantity"`

	// Насос
	FlowRate    *float64 `json:"flow_rate,omitempty"`
	FluidHead   *float64 `json:"fluid_head,omitempty"`
	RPM         *float64 `json:"rpm,omitempty"`
	SpecGravity *float64 `json:"spec_gravity,omitempty"`
	PowerKW     *float64 `json:"power_kw,omitempty"`

	// Конвейер
	ConveyorLength   *float64 `json:"conveyor_length,omitempty"`
	BeltWidth        *float64 `json:"belt_width,omitempty"`
	ConveyorFlowRate *float64 `json:"conveyor_flow_rate,omitempty"`

	// Vessel/Drum
	VesselDiameter               *float64 `json:"vessel_diameter,omitempty"`
	DesignTangentToTangentLength *float64 `json:"design_tangent_to_tangent_length,omitempty"`
	VesselTangentToTangentHeight *float64 `json:"vessel_tangent_to_tangent_height,omitempty"`
	DesignGaugePressure          *float64 `json:"design_gauge_pressure,omitempty"`
	DesignTemperature            *float64 `json:"design_temperature,omitempty"`
	SkirtHeight                  *float64 `json:"skirt_height,omitempty"`
	VesselLegHeight              *float64 `json:"vessel_leg_height,omitempty"`

	CalculatedWeight float64 `json:"calculated_weight"`
}

type Project struct {
	Name      string      `json:"name"`
	Equipment []Equipment `json:"equipment"`
}

func (p Project) TotalWeight() float64 {
	var total float64
	for _, eq := range p.Equipment {
		total += eq.CalculatedWeight * float64(eq.Quantity)
	}
	return total
}

func (p Project) EquipmentCount() int {
	var count int
	for _, eq := range p.Equipment {
		count += eq.Quantity
	}
	return count
}

type AppData struct {
	Projects []Project `json:"projects"`
}
