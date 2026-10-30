package domain

import "time"

const DefaultLocale = "en"

type Profile struct {
	UserID   string `dynamodbav:"user_id" json:"user_id"`
	Locale   string `dynamodbav:"locale,omitempty" json:"locale,omitempty"`
	Timezone string `dynamodbav:"timezone,omitempty" json:"timezone,omitempty"`
	Email    string `dynamodbav:"email,omitempty" json:"email,omitempty"`
	Phone    string `dynamodbav:"phone,omitempty" json:"phone,omitempty"`
}

type Preference struct {
	Category   Category `dynamodbav:"category" json:"category"`
	Channel    Channel  `dynamodbav:"channel" json:"channel"`
	Enabled    bool     `dynamodbav:"enabled" json:"enabled"`
	QuietStart string   `dynamodbav:"quiet_start,omitempty" json:"quiet_start,omitempty"`
	QuietEnd   string   `dynamodbav:"quiet_end,omitempty" json:"quiet_end,omitempty"`
}

type Device struct {
	ID       string `dynamodbav:"device_id" json:"device_id"`
	Token    string `dynamodbav:"token" json:"-"`
	Platform string `dynamodbav:"platform" json:"platform"`
	Active   bool   `dynamodbav:"active" json:"active"`
}

type UserData struct {
	Found   bool         `json:"found"`
	Profile Profile      `json:"profile"`
	Prefs   []Preference `json:"prefs"`
	Devices []Device     `json:"devices"`
}

// Pref returns the stored preference for a category and channel, if any.
func (u UserData) Pref(c Category, ch Channel) (Preference, bool) {
	for _, p := range u.Prefs {
		if p.Category == c && p.Channel == ch {
			return p, true
		}
	}
	return Preference{}, false
}

// ActiveDevices lists devices that can still receive push.
func (u UserData) ActiveDevices() []Device {
	var out []Device
	for _, d := range u.Devices {
		if d.Active {
			out = append(out, d)
		}
	}
	return out
}

// Location resolves the user's timezone, falling back to UTC.
func (p Profile) Location() *time.Location {
	if loc, err := time.LoadLocation(p.Timezone); err == nil && p.Timezone != "" {
		return loc
	}
	return time.UTC
}

type Template struct {
	ID      string  `dynamodbav:"template_id" json:"template_id"`
	Channel Channel `dynamodbav:"channel" json:"channel"`
	Locale  string  `dynamodbav:"locale" json:"locale"`
	Version int     `dynamodbav:"version" json:"version"`
	Subject string  `dynamodbav:"subject,omitempty" json:"subject,omitempty"`
	Body    string  `dynamodbav:"body" json:"body"`
}
