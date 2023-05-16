package main

import (
	"log"
	"os"

	"github.com/GoogleCloudPlatform/functions-framework-go/funcframework"

	_ "example.com/billing-export-notification"
)

func main() {
	// Use PORT environment variable, or default to 8080.
	port := "8080"
	if envPort := os.Getenv("PORT"); envPort != "" {
		port = envPort
	}

	// functionTarget := "BillingNotiFunc"
	if envFunctionTarget := os.Getenv("FUNCTION_TARGET"); envFunctionTarget == "" {
		// functionTarget = envFunctionTarget
		os.Setenv("FUNCTION_TARGET", "BillingNotiFunc")
	}

	if err := funcframework.Start(port); err != nil {
		log.Fatalf("funcframework.Start: %v\n", err)
	}

}