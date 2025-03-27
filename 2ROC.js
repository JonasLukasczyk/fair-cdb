// const cdb_path = '/home/jones/2tb/data/jet4.cdb';
const cdb_path = '/home/jones/2tb/data/jet4-benchmark-localized-topological-simplification-main/';
// const cdb_path = '/home/jones/2tb/data/jet-data-new/';

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

const prompt_cmdline = question => {
  const rl = readline.createInterface({
      input: process.stdin,
      output: process.stdout
  });

  return new Promise(resolve => {
      rl.question(question, answer => {
          rl.close();
          resolve(answer);
      });
  });
};


const root_files = fs.readdirSync(cdb_path);

const root_dataset = {
  '@id': './',
  '@type': 'Dataset',
  'variableMeasured': [],
  'hasPart': []
};
roc['@graph'].push(root_dataset);

const persons = new Map();
const organizations = new Map();
const terms = new Map();
const termValues = new Map();
const files = new Map();
const variables = new Map();

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

const prompt_choice = async (name,suggestions)=>{
  let title = '';
  title += `-------------------------------------------------------------------------\n`;
  title += `Please provide an Ontology Term Identifier for data.csv column "${name}".\n`;

  // suggestions.push(['None','use the column name as an undefined term','']);

  title += '  Suggestions:\n';
  for(let i=0; i<suggestions.length; i++)
    title += `    ${i+1}) ${suggestions[i][0]}: ${suggestions[i][1]}\n       ${suggestions[i][2]}\n`;
  title += 'TermIdentifier: ';

  const answer = await prompt_cmdline(title);
  if(answer==='')
    return {
      '@id':'#'+name.split(' ').join('_'),
      name:name,
    };

  // TODO what if user entered text instead of index
  const answerAsIndex = parseInt(answer)-1;
  return {
    '@id':suggestions[answerAsIndex][3],
    identifier:suggestions[answerAsIndex][0],
    name:suggestions[answerAsIndex][1],
  };
};

const suggestions = {
  'time': [
    ['APOLLO_SV_00000069','time step identifying numeral','The time step number of a simulation.','http://purl.obolibrary.org/obo/APOLLO_SV_00000069'],
    ['APOLLO_SV_00000272','time since time scale zero','A duration of time that has elapsed since the zero reference point of a time scale.','http://purl.obolibrary.org/obo/APOLLO_SV_00000272'],
    ['OMRSE_00000136','date','The only valid string values for this property are ISO 8601 formatted date strings in extended form. It is allowable specify only the year, e.g. "2016" but only when the 1D temporal region references the entire year. Ditto for month, e.g. "2016-04" is acceptable but only if it references the entire interval of that month.'],
  ],
  'angle': [
    ['TODO','TODO','TODO'],
  ],
};

const createTermValue = (termKey,value)=>{
  const id = termKey+'_'+value;
  if(termValues.has(id)) return id;

  const term = terms.get(termKey);

  const termValue =  {
    '@id': id,
    '@type': 'PropertyValue',
    'propertyId': term['@id'],
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
    '@type': ['File','Sample'],
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
        variables.set(line.split('Name="')[1].split('"')[0],null);
      }
    }
  }

  files.set(id,file);
  roc['@graph'].push(file);
  root_dataset.hasPart.push({'@id':id});
  return id;
};

const getOntologyTerm = async (name,target)=>{
  const name_lower = name.toLowerCase();
  return await prompt_choice(
    name,
    ['t','time','timestep'].includes(name_lower) || name_lower.includes('time')
      ? suggestions.time
      : name_lower.includes('step')
        ? [suggestions.time[0]]
        : []
  );
};

const processCSV = async file => {
  const csv_raw = fs.readFileSync(cdb_path+'/'+file,'UTF-8').split('\n');
  const columns = csv_raw[0].split(',').map(i=>i.trim());
  for(let column of columns){
    if(column.toLowerCase().includes('file')) continue;

    terms.set(column,await getOntologyTerm(column));
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
    const term = await getOntologyTerm(name);
    variables.set(name,term);
    term['@type'] = 'Property';
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
//       'propertyId': term.termCode,
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
