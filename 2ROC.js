import fs from 'fs';
import readline from 'readline';
import yaml from 'js-yaml';
import XMLImageDataReader from '@kitware/vtk.js/IO/XML/XMLImageDataReader.js';

const cdb_path = process.argv[2]

const root_files = fs.readdirSync(cdb_path);
const default_date = new Date();
const default_name = cdb_path.split('/').at(-2);


const roc = {
  "@context": "https://w3id.org/ro/crate/1.1/context",
  "@graph": [
    {
        "@type": "CreativeWork",
        "@id": "ro-crate-metadata.json",
        "conformsTo": {"@id": "https://w3id.org/ro/crate/1.1"},
        "about": {"@id": "./"}
    }
  ]
};

const root_dataset = {
  '@id': './',
  '@type': 'Dataset',
  'variableMeasured': [],
  'hasPart': [],
  'license': 'ALL RIGHTS RESERVED BY THE AUTHORS',
  'datePublished': default_date.toISOString(),
  'name': default_name
};
roc['@graph'].push(root_dataset);

const persons = new Map();
const organizations = new Map();
const terms = new Map();
const termValues = new Map();
const files = new Map();
const variables = new Map();
const userParameters = new Map();
const userVariables = new Map();
const userVariableAnnotations = new Map();

const getFieldDataArrays = filePath => {
  const fileBuffer = fs.readFileSync(filePath);
  const reader = XMLImageDataReader.newInstance();
  reader.parseAsArrayBuffer(fileBuffer);
  const imageData = reader.getOutputData();
  const fieldData = imageData.getFieldData();

  let map = new Map();

  for (let i = 0; i < fieldData.getNumberOfArrays(); i++) {
      const array = fieldData.getArray(i);
      const nv = array.getNumberOfValues();
      if(nv==1){
        const name = array.getName();
        map.set(name,array.getData().slice(0, 1)[0]);
      }
  }

  return map;
};

const cffAffiliation2SchemaOrganization = cffAffiliation => {
  const id = '#'+encodeURI(cffAffiliation);
  if(organizations.has(id)) return id;

  const organization = {
    '@id': id,
    'name': cffAffiliation
  };
  organizations.set(id,organization);
  roc['@graph'].push(organization);

  return id;
};

const cffPerson2SchemaPerson = cffPerson => {
  const id = cffPerson.orcid ? cffPerson.orcid : '#'+encodeURI(cffPerson['given-names']+'_'+cffPerson['family-names']);
  if(persons.has(id)) return id;

  const person = {
    '@id': id,
    '@type': 'Person',
    'givenName': cffPerson['given-names'],
    'familyName': cffPerson['family-names']
  };
  if(cffPerson.affiliation)
    person.affiliation = [{'@id': cffAffiliation2SchemaOrganization(cffPerson.affiliation)}];

  persons.set(id,person);
  roc['@graph'].push(person);
  return id;
};

const processCFF = file => {
  const cff_raw = fs.readFileSync(cdb_path+'/'+file,'UTF-8');
  const cff = yaml.load(cff_raw);
  for(let key of Object.keys(cff)){
    switch (key) {
      case 'title':
        root_dataset.title = cff.title;
        root_dataset.name = cff.title;
        break;
      case 'abstract':
        root_dataset.description = cff.abstract;
        break;
      case 'license':
        root_dataset.license = cff.license;
        break;
      case 'date-published':
        root_dataset.datePublished = cff['date-published'];
        break;
      case 'keywords':
        root_dataset.keywords = cff['keywords'].join(', ');
        break;
      case 'authors':
        root_dataset.creator = [];
        for(let author of cff.authors)
          root_dataset.creator.push(
            {'@id':cffPerson2SchemaPerson(author)}
          );
        break;
    }
  }
};

const processTerms = file => {
  const terms_yml = fs.readFileSync(cdb_path+'/'+file,'UTF-8');
  const term_assignment = yaml.load(terms_yml);
  for(let key of Object.keys(term_assignment['parameters'])){
    const term = term_assignment['parameters'][key];
    let term_object = {
      '@id': "#"+key.split(' ').join('_'),
      '@type': 'PropertyValue',
      'name': key
    };
    if(term['term']){
      term_object['@id'] = term['term'];
      term_object.identifier = term['term'];
    }
    if(term['description'])
      term_object.description = term['description'];
    if(term['identifier'])
      term_object.propertyID = term['identifier'];
    if(term['name'])
      term_object.name = term['name'];
    userParameters.set(key,term_object);
  }
  for(let key of Object.keys(term_assignment['variables'])){
    const term = term_assignment['variables'][key];
    let term_object = {
      '@id': "#"+key.split(' ').join('_'),
      '@type': 'PropertyValue',
      'name': key
    };
    if(term['term']){
      term_object['@id'] = term['term'];
      term_object.identifier = term['term'];
    }
    if(term['description'])
      term_object.description = term['description'];
    if(term['identifier'])
      term_object.propertyID = term['identifier'];
    if(term['name'])
      term_object.name = term['name'];
    if(term['forced'])
      userVariables.set(key.toLowerCase(),term_object);
    else
      userVariableAnnotations.set(key.toLowerCase(),term_object);
  }
};

const createTermValue = (termKey,value)=>{
  const id = termKey+'_'+value;
  if(termValues.has(id)) return id;

  const term = terms.get(termKey);

  const termValue =  {
    '@id': id,
    '@type': 'PropertyValue',
    'propertyID': term['@id'],
    'description': term.description,
    'identifier': term.identifier,
    'name': term.name,
    'value': value
  };
  termValues.set(id,termValue);
  roc['@graph'].push(termValue);
  return id;
};

const createFile = async (path,file_terms)=>{
  const id = './'+path;
  if(files.has(id)) return id;

  const file = {
    '@id': id,
    '@type': ['File','https://bioschemas.org/Sample'],
    'encodingFormat': path.split('.').pop(),
    'additionalProperty': []
  };

  for(let term of file_terms)
    file.additionalProperty.push({'@id':term});

  if(path.endsWith('vti')){

    const fileStream = fs.createReadStream(cdb_path+'/'+path);
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity // This handles both \n and \r\n line endings
    });
    // rl.on('line', (line) => {
    //   console.log(`Line: ${line}`);
    // });
    for await(const line of rl) {
      if(line.includes('<AppendedData')) break;
      if(line.includes('<DataArray')){
        const name = line.split('Name="')[1].split('"')[0];
        variables.set(name,null);
      }
    }
    const fieldData = getFieldDataArrays(cdb_path+'/'+path);
    for(let key of fieldData.keys()){
      const term = userParameters.get(key);
      terms.set(key,term);
      if(!term) continue;
      const termValue = createTermValue(key,fieldData.get(key));
      console.log('termValue',termValue);
      console.log('file.additionalProperty',file.additionalProperty);
      file.additionalProperty.push({'@id':termValue});
      console.log('file.additionalProperty',file.additionalProperty);
    }
  }

  files.set(id,file);
  roc['@graph'].push(file);
  root_dataset.hasPart.push({'@id':id});
  return id;
};

const processCSV = async file => {
  const csv_raw = fs.readFileSync(cdb_path+'/'+file,'UTF-8').split('\n');
  const columns = csv_raw[0].split(',').map(i=>i.trim());
  for(let column of columns){
    if(column.toLowerCase().includes('file')) continue;

    if(userParameters.has(column)){
      const term = userParameters.get(column);
      terms.set(column,term);
    }
    else{
      const term = {
        '@id':'#'+column.split(' ').join('_'),
        '@type':'PropertyValue',
        name:column,
      };
      terms.set(column,term);
    }

  }

  for(let i=1; i<csv_raw.length; i++){
    if(csv_raw[i]==='') continue;
    const values = csv_raw[i].split(',').map(i=>i.trim());
    const terms_in_line = [];
    for(let j=0; j<values.length; j++){
      if(!columns[j].includes('FILE'))
        terms_in_line.push(
          createTermValue(columns[j],values[j])
        );
    }
    for(let j=0; j<values.length; j++){
      if(columns[j].includes('FILE'))
        await createFile(values[j],terms_in_line);
    }
  }

  for(let name of variables.keys()){
    if(name in terms.keys()){
      const term = userVariableAnnotations[key];
      variables.set(name,term);
      root_dataset.variableMeasured.push({'@id': term['@id']});
      roc['@graph'].push(term);
    }
  }

  for(let name of userVariables.keys()){
    const term = userVariables.get(name);
    variables.set(name,term);
    root_dataset.variableMeasured.push({'@id': term['@id']});
    roc['@graph'].push(term);
  }

};

const processCWL = async file => {
  const cwl_raw = fs.readFileSync(cdb_path+'/'+file,'UTF-8').split('\n');

  const cwl = {
    '@id': './'+file,
    '@type': ['File','SoftwareSourceCode','ComputationalWorkflow'],
    'name': file.split('.')[0],
  };

  root_dataset.hasPart.push({'@id':cwl['@id']});
  roc['@graph'].push(cwl);
};

const init = async ()=>{

  if(root_files.includes('terms.yml')){ 
    await processTerms("terms.yml");
  }

  // process files
  for(let file of root_files){
    if(file==='data.csv')
      await processCSV(file);
    else if(file.endsWith('.cff'))
      await processCFF(file);
    else if(file.endsWith('.cwl'))
      await processCWL(file);
  }

  const res = JSON.stringify(roc,null,2);
  fs.writeFileSync(cdb_path+'/ro-crate-metadata.json', res);

  // console.log(res)
};

init();