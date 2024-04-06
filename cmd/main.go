package main

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/charmbracelet/huh"
	"github.com/google/uuid"
	"github.com/pelletier/go-toml/v2"
)

var CLI struct {
	Init struct {
		Table struct {
			Name string `help:"Name of the table."`
		} `cmd:"" help:"Initialize a new table."`
	} `cmd:"" help:"Initialize a new project or resource."`
}

func main() {
	ctx := kong.Parse(&CLI)
	switch ctx.Command() {
	case "init table":
		name := CLI.Init.Table.Name

		if name == "" {
			form := huh.NewForm(
				huh.NewGroup(
					huh.NewInput().
						Title("Table Name").
						Placeholder("Enter the name of the table.").
						Validate(func(s string) error {
							if s == "" {
								return fmt.Errorf("table name cannot be empty")
							}
							return nil
						}).
						Value(&name),
				),
			)

			if err := form.Run(); err != nil {
				fmt.Println("Error:", err)
				return
			}
		}

		fmt.Println("Table Name:", name)
		cfg, err := toml.Marshal(TableSchema{
			Id:   uuid.New(),
			Name: name,
			Fields: map[string]TableField{
				"id": {
					Id:        uuid.New(),
					DataType:  DataTypeUUID,
					Unique:    true,
					Required:  true,
					Automatic: true,
				},
				"created_at": {
					Id:        uuid.New(),
					DataType:  DataTypeTimestamp,
					Required:  true,
					Automatic: true,
				},
				"updated_at": {
					Id:        uuid.New(),
					DataType:  DataTypeTimestamp,
					Required:  true,
					Automatic: true,
				},
			},
		})

		if err != nil {
			fmt.Println("Error:", err)
			return
		}

		fmt.Println(string(cfg))

	default:
		panic(ctx.Command())
	}
}

type DataType string

const (
	DataTypeUUID      DataType = "uuid"
	DataTypeTimestamp DataType = "timestamp"
)

type TableSchema struct {
	Id     uuid.UUID             `toml:"id"`
	Name   string                `toml:"name"`
	Fields map[string]TableField `toml:"fields"`
}

type TableField struct {
	Id        uuid.UUID `toml:"id"`
	DataType  DataType  `toml:"type"`
	Unique    bool      `toml:"unique"`
	Required  bool      `toml:"required"`
	Automatic bool      `toml:"automatic"`
}
