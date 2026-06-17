package route

import "testing"

func TestNormalizeTraefikProfile(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		profile string
		want    string
	}{
		{
			name:    "empty",
			profile: "",
			want:    "",
		},
		{
			name:    "lowercase kept",
			profile: "rstudio",
			want:    "rstudio",
		},
		{
			name:    "trim and lowercase",
			profile: "  SCode  ",
			want:    "scode",
		},
		{
			name:    "supports notebook",
			profile: "NOTEBOOK",
			want:    "notebook",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := normalizeTraefikProfile(tc.profile); got != tc.want {
				t.Fatalf("normalizeTraefikProfile() = %q, want %q", got, tc.want)
			}
		})
	}
}
