"""Construct a bounded read-only Node command; the existing job owner executes it."""

import json
from pathlib import PurePosixPath

from jsonschema import ValidationError, validate

from .docker_runner import DockerSandbox


SOURCE_READ_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["sections"],
    "properties": {"sections": {"type": "array", "minItems": 1, "maxItems": 16, "items": {
        "type": "object", "additionalProperties": False,
        "required": ["path", "start_line", "line_count", "match"],
        "properties": {
            "path": {"type": "string", "minLength": 1, "maxLength": 512},
            "start_line": {"type": "integer", "minimum": 1, "maximum": 1000000},
            "line_count": {"type": "integer", "minimum": 1, "maximum": 500},
            "match": {"type": ["string", "null"], "minLength": 1, "maxLength": 256},
        },
    }}},
}

_PROGRAM = r"""
const fs=require('node:fs'),path=require('node:path');
const input=JSON.parse(process.argv[1]),root=fs.realpathSync(process.cwd());
const maxFile=1024*1024;
function read(q,index){
  let fd;
  try{
    const real=fs.realpathSync(path.resolve(root,q.path));
    if(!real.startsWith(root+path.sep))return {index,status:'outside_workspace'};
    fd=fs.openSync(real,fs.constants.O_RDONLY|fs.constants.O_NOFOLLOW|fs.constants.O_NONBLOCK);
    const stat=fs.fstatSync(fd);
    if(!stat.isFile())return {index,status:'not_file'};
    if(stat.size>maxFile)return {index,status:'file_too_large'};
    const buffer=Buffer.alloc(maxFile+1);let size=0,n;
    while(size<buffer.length&&(n=fs.readSync(fd,buffer,size,buffer.length-size,null)))size+=n;
    if(size>maxFile)return {index,status:'file_too_large'};
    const raw=buffer.subarray(0,size),text=raw.toString('utf8');
    if(!Buffer.from(text,'utf8').equals(raw))return {index,status:'invalid_utf8'};
    const lines=text.split(/\r?\n/);if(lines.at(-1)==='')lines.pop();
    function excerpt(start){
      const selected=lines.slice(start,start+q.line_count);
      return {start_line:start+1,total_lines:lines.length,
              text:selected.join('\n'),line_count:selected.length};
    }
    let start=q.start_line-1;
    if(q.match!==null){
      const from=start;
      start=lines.findIndex((line,i)=>i>=from&&line.includes(q.match));
      if(start<0){
        const words=q.match.match(/[\p{L}\p{N}_]+/gu)||[];
        const nearby=words.length<2?-1:lines.findIndex((line,i)=>{
          if(i<from)return false;
          let at=0;
          for(const word of line.match(/[\p{L}\p{N}_]+/gu)||[]){
            if(word===words[at]&&++at===words.length)return true;
          }
          return false;
        });
        return nearby<0?{index,status:'no_match'}:
          {index,status:'no_match',nearby:{match_kind:'ordered_tokens',...excerpt(nearby)}};
      }
    }
    if(start>=lines.length&&!(start===0&&lines.length===0))return {index,status:'out_of_range'};
    return {index,status:'ok',...excerpt(start)};
  }catch{return {index,status:'unreadable'}}
  finally{if(fd!==undefined)fs.closeSync(fd)}
}
// Reserve every request's omission marker first; a large section cannot erase others.
const sections=input.sections.map((_,index)=>({index,status:'omitted_budget'}));
const output={sections};
if(Buffer.byteLength(JSON.stringify(output))>input.limit)throw Error('Unavailable output budget');
for(let i=0;i<sections.length;i++){
  const previous=sections[i];sections[i]=read(input.sections[i],i);
  if(Buffer.byteLength(JSON.stringify(output))>input.limit&&sections[i].nearby){
    delete sections[i].nearby;sections[i].nearby_omitted_budget=true;
  }
  if(Buffer.byteLength(JSON.stringify(output))>input.limit)sections[i]=previous;
}
process.stdout.write(JSON.stringify(output));
"""


def source_read_command(arguments: dict) -> list[str]:
    """Validate selection only, without reading the controller filesystem."""
    try:
        validate(arguments, SOURCE_READ_SCHEMA)
        for section in arguments["sections"]:
            name = section["path"]
            path = PurePosixPath(name)
            if not path.parts or path.is_absolute() or ".." in path.parts or "\\" in name or "\x00" in name:
                raise ValueError("Source path must remain relative")
    except (ValidationError, TypeError, ValueError):
        raise ValueError("Invalid source inspection request") from None
    limit = DockerSandbox.MAX_CAPTURED_OUTPUT_BYTES - 256
    if limit < 1024:
        raise ValueError("Source inspection output budget unavailable")
    payload = json.dumps({**arguments, "limit": limit}, ensure_ascii=False)
    try:
        payload.encode("utf-8")
    except UnicodeError:
        raise ValueError("Invalid source inspection encoding") from None
    return ["node", "-e", _PROGRAM, payload]
