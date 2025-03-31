// const cdb_path = '/home/jones/2tb/data/jet4.cdb';
// const cdb_path = '/home/jones/2tb/data/jet4-benchmark-localized-topological-simplification-main/';
// const cdb_path = '/home/jones/2tb/data/jet-data-new/';
const cdb_path = '/home/wetzels/jet4-benchmark-localized-topological-simplification/';

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

// process cff

const fs = require('fs');
const yaml = require('js-yaml');
const readline = require('readline');


const root_files = fs.readdirSync(cdb_path);

const default_date = new Date();
const default_name = cdb_path.split('/').at(-2);

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

const cffAffiliation2SchemaOrganization = cffAffiliation => {
  const id = encodeURI(cffAffiliation);
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
  if(cffPerson.orcid) return cffPerson.orcid;

  const id = encodeURI(cffPerson['given-names']+'_'+cffPerson['family-names']);
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
    term_object = {
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
    term_object = {
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

// processTerms('terms.yml');
// console.log('userParameters',userParameters);
// console.log('userVariables',userVariables);
// console.log('userVariableAnnotations',userVariableAnnotations);

// const prompt_choice = async (name,suggestions)=>{
//   let title = '';
//   title += `-------------------------------------------------------------------------\n`;
//   title += `Please provide an Ontology Term Identifier for data.csv column "${name}".\n`;

//   // suggestions.push(['None','use the column name as an undefined term','']);

//   title += '  Suggestions:\n';
//   for(let i=0; i<suggestions.length; i++)
//     title += `    ${i+1}) ${suggestions[i][0]}: ${suggestions[i][1]}\n       ${suggestions[i][2]}\n`;
//   title += 'TermIdentifier: ';

//   const answer = await prompt_cmdline(title);
//   if(answer==='')
//     return {
//       '@id':'#'+name.split(' ').join('_'),
//       name:name,
//     };

//   // TODO what if user entered text instead of index
//   const answerAsIndex = parseInt(answer)-1;
//   return {
//     '@id':suggestions[answerAsIndex][3],
//     identifier:suggestions[answerAsIndex][0],
//     name:suggestions[answerAsIndex][1],
//   };
// };

// const prompt_variable = async ()=>{
//   let title = '';
//   title += `-------------------------------------------------------------------------\n`;
//   title += `Please provide additional variables represented in the data if possible.\n`;
//   title += 'Variable Name: ';

//   const name = await prompt_cmdline(title);
//   if(!(name==='')){
//     title = 'Variable Term Identifier:';
//     const url = await prompt_cmdline(title);
//     if(!(url==='')){
//       return {
//         '@id':url,
//         '@type':'PropertyValue',
//         propertyID:url,
//         name:name,
//       };
//     }
//     else{
//       return {
//         '@id':'#'+,name.split(' ').join('_')
//         '@type':'PropertyValue',
//         name:name,
//       };
//     }
//   }
//   else{
//     return;
//   }

//   // TODO what if user entered text instead of index
//   const answerAsIndex = parseInt(answer)-1;
//   return {
//     '@id':suggestions[answerAsIndex][3],
//     identifier:suggestions[answerAsIndex][0],
//     name:suggestions[answerAsIndex][1],
//   };
// };

// const suggestions = {
//   'time': [
//     ['APOLLO_SV_00000069','time step identifying numeral','The time step number of a simulation.','http://purl.obolibrary.org/obo/APOLLO_SV_00000069'],
//     ['APOLLO_SV_00000272','time since time scale zero','A duration of time that has elapsed since the zero reference point of a time scale.','http://purl.obolibrary.org/obo/APOLLO_SV_00000272'],
//     ['OMRSE_00000136','date','The only valid string values for this property are ISO 8601 formatted date strings in extended form. It is allowable specify only the year, e.g. "2016" but only when the 1D temporal region references the entire year. Ditto for month, e.g. "2016-04" is acceptable but only if it references the entire interval of that month.'],
//   ],
//   'angle': [
//     ['TODO','TODO','TODO'],
//   ],
// };

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

const createFile = async (path,terms)=>{
  const id = './'+path;
  if(files.has(id)) return id;

  const file = {
    '@id': id,
    '@type': ['File','https://bioschemas.org/Sample'],
    'encodingFormat': path.split('.').pop(),
    'additionalProperty': []
  };

  for(let term of terms)
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
  }

  files.set(id,file);
  roc['@graph'].push(file);
  root_dataset.hasPart.push({'@id':id});
  return id;
};

// const getOntologyTerm = async (name,target)=>{
//   const name_lower = name.toLowerCase();
//   return await prompt_choice(
//     name,
//     ['t','time','timestep'].includes(name_lower) || name_lower.includes('time')
//       ? suggestions.time
//       : name_lower.includes('step')
//         ? [suggestions.time[0]]
//         : []
//   );
// };

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

  // for(let i=1; i<3; i++){
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
      // term['@type'] = 'Property';
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
};

init();



// const root_dataset = {
//   '@id': './',
//   '@type': 'Dataset',
//   'hasPart': []
// };
// roc['@graph'].push(dataset);


// const csv = fs.readFileSync('./data.csv', 'utf8');
// const lines = csv.split('\n');
// const header = lines[0].split(',').map(x=>x.trim());
// const rows = lines.slice(1);

// const header_map = {
//   'Sim': {
//     'termCode': 'http://purl.obolibrary.org/obo/IAO_0020000',
//     'name':'identifier'
//   },
//   'Time': {
//     'termCode': 'http://purl.obolibrary.org/obo/APOLLO_SV_00000069',
//     'name':'simulatorTime'
//   },
// };

// const nColumns = header.length;


// let counter = 0;
// for(let row of rows){
//   const items = row.split(',').map(x=>x.trim());
//   if(items.length!==nColumns) continue;

//   if(counter++>5) break;

//   const rowAsSchemaFile = {
//     '@id': './'+items[2],
//     '@type': ['File','Sample'],
//     'encodingFormat': 'vti',
//     'additionalProperty': []
//   };

//   for(let i=0; i<nColumns; i++){
//     const name = header[i];
//     if(name==='FILE') continue;

//     const term = header_map[name];
//     const property = {
//       '@id': items[2]+'_'+i,
//       '@type': 'PropertyValue',
//       'propertyID': term.termCode,
//       'name': term.name,
//       'value': items[i]
//     };

//     roc['@graph'].push(property);
//     rowAsSchemaFile.additionalProperty.push({'@id': property['@id']});
//   }

//   dataset.hasPart.push({'@id': rowAsSchemaFile['@id']});

//   roc['@graph'].push(rowAsSchemaFile);
// }

// console.log(roc);

// fs.writeFileSync('ro-crate-metadata.json', JSON.stringify(roc,null,2));
