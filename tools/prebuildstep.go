package main

import (
	"encoding/base64"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/jteeuwen/go-bindata"
)

func getCheckSuiteFiles() map[string][]string {
	relPath := "./checksuite/"
	absPath, err := filepath.Abs(relPath)
	if err != nil {
		panic(err)
	}

	checkPkgs := make(map[string][]string)

	filepath.Walk(absPath, func(path string, info os.FileInfo, err error) error {
		// Ignore any directories
		if info.IsDir() {
			return nil
		}

		// Calculate the relative file path
		checksRelPath, _ := filepath.Rel(absPath, path)

		// Get the directory and file components
		checksDir, checksFile := filepath.Split(checksRelPath)

		// Ignore any files in the root checksuite directory
		if checksDir == "" {
			return nil
		}

		// Strip the trailing slash off the directory
		checksDir = checksDir[:len(checksDir)-1]

		checkPkgs[checksDir] = append(checkPkgs[checksDir], checksFile)

		return nil
	})

	return checkPkgs
}

type suiteCheckFunc struct {
	AbsPkgName  string
	RelPkgName  string
	CheckName   string
	FuncName    string
	Description string
}

func getCheckSuiteFuncs() ([]suiteCheckFunc, error) {
	suitePkgs := getCheckSuiteFiles()
	fmt.Printf("%+v\n", suitePkgs)

	var suiteChecks []suiteCheckFunc

	for suiteName := range suitePkgs {
		suitePath := fmt.Sprintf("./checksuite/%s", suiteName)

		fset := token.NewFileSet()

		pkgs, err := parser.ParseDir(fset, suitePath, nil, parser.ParseComments)
		if err != nil {
			log.Fatalf("failed to parse checksuite packages: %s", err)
		}

		if len(pkgs) != 1 {
			log.Fatalf("found more than one package name per checksuite directory")
		}

		var suitePkg *ast.Package
		for _, pkg := range pkgs {
			suitePkg = pkg
		}

		suiteFile := ast.MergePackageFiles(suitePkg, ast.MergeMode(0))

		for _, suiteDecl := range suiteFile.Decls {
			if suiteFunc, ok := suiteDecl.(*ast.FuncDecl); ok {
				// Ignore methods which have receivers (methods not functions)
				if suiteFunc.Recv != nil {
					continue
				}

				// Grab the name of the function
				funcName := suiteFunc.Name.String()

				// Ignore functions which aren't Check functions
				if !strings.HasPrefix(funcName, "Check") {
					continue
				}

				// Strip the check prefix
				checkName := funcName[5:]

				// Fetch the description of the test
				checkDesc := suiteFunc.Doc.Text()

				// Add this to the list of tests to store
				suiteChecks = append(suiteChecks, suiteCheckFunc{
					AbsPkgName:  "github.com/couchbaselabs/gocaves/checksuite/" + suiteName,
					RelPkgName:  suiteName,
					CheckName:   checkName,
					FuncName:    funcName,
					Description: checkDesc,
				})
			}
		}
	}

	return suiteChecks, nil
}

func pkgIDFromName(pkgName string) string {
	pkgName = strings.ReplaceAll(pkgName, "/", "_")
	pkgName = strings.ReplaceAll(pkgName, "\\", "_")
	return "check_" + pkgName
}

type checkFuncPkg struct {
	PkgID      string
	RelPkgName string
	AbsPkgName string
}

func updateCheckSuiteDotGo() {
	suiteChecks, err := getCheckSuiteFuncs()
	if err != nil {
		log.Fatalf("failed to parse check suite packages: %s", err)
	}

	pkgInfos := make(map[string]checkFuncPkg)
	for _, checkFunc := range suiteChecks {
		pkgInfos[checkFunc.RelPkgName] = checkFuncPkg{
			PkgID:      pkgIDFromName(checkFunc.RelPkgName),
			RelPkgName: checkFunc.RelPkgName,
			AbsPkgName: checkFunc.AbsPkgName,
		}
	}

	indexOut := ""

	indexOut += "package checksuite\n"
	indexOut += "\n"

	indexOut += "// This file is autogenerated by the prebuildstep tool.\n"
	indexOut += "// Run it using: `go run tools/prebuildstep.go`\n"
	indexOut += "\n"

	indexOut += "import (\n"

	indexOut += "\tregistry \"github.com/couchbaselabs/gocaves/checks\"\n"
	indexOut += "\n"

	for _, pkgInfo := range pkgInfos {
		indexOut += "\t" + pkgInfo.PkgID + " \"" + pkgInfo.AbsPkgName + "\"\n"
	}
	indexOut += ")\n"

	indexOut += "\n"

	indexOut += "// RegisterCheckFuncs registers all test suite methods\n"
	indexOut += "func RegisterCheckFuncs() {\n"

	for _, checkFunc := range suiteChecks {
		pkgInfo := pkgInfos[checkFunc.RelPkgName]

		indexOut += fmt.Sprintf("\tregistry.Register(\"%s\", \"%s\", %s,\n\t\t\"%s\")\n",
			pkgInfo.RelPkgName,
			checkFunc.CheckName,
			pkgInfo.PkgID+"."+checkFunc.FuncName,
			base64.StdEncoding.EncodeToString([]byte(checkFunc.Description)))
	}

	//	registerTest("crud/Get", check_crud.CheckGet)
	indexOut += "}"
	indexOut += "\n"

	ioutil.WriteFile("./checksuite/checksuite.go", []byte(indexOut), 0644)
}

func updateBindataFiles() {
	cfg := bindata.NewConfig()
	cfg.Output = "mock/data/gobindata.go"
	cfg.Package = "mockdata"
	cfg.Prefix = "mock/data"
	cfg.Input = []bindata.InputConfig{
		{Path: "mock/data"},
	}
	bindata.Translate(cfg)
}

func main() {
	updateCheckSuiteDotGo()
	updateBindataFiles()
}
